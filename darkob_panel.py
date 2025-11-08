# -*- coding: utf-8 -*-
"""
Darkob — Live User Activity Panel (High-Performance Edition)


وابستگی‌ها:
  pip install fastapi uvicorn
"""

import argparse
import asyncio
import json
import logging
import os
import re
from collections import deque
from typing import AsyncGenerator, Deque, Dict, List, Optional, Set, Tuple

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    StreamingResponse,
)

# -----------------------------
# تنظیمات
# -----------------------------
LOG_PATH = os.getenv("LOG_PATH", "/root/V2IpLimit/connections_log.txt")
USER_STATS_FILE = os.getenv("USER_STATS_FILE", "/root/V2IpLimit/user_stats.json")

# تعداد رخدادهایی که فقط برای "سرویس‌دهی سریع" و "LIVE" در حافظه نگه می‌داریم
MAX_RECENT_EVENTS = int(os.getenv("MAX_RECENT_EVENTS", "5000"))

# حداکثر تعداد hostهای نمونه که برای جست‌وجوی سایت برای هر کاربر ذخیره می‌کنیم
MAX_HOSTS_PER_USER = int(os.getenv("MAX_HOSTS_PER_USER", "200"))

# فاصله‌ی heartbeat برای SSE
SSE_HEARTBEAT_SEC = 15

# --- Full Scan Throttling ---
FULL_SCAN_SLEEP_EVERY = int(os.getenv("FULL_SCAN_SLEEP_EVERY", "3000"))  # هر چند خط یک مکث
FULL_SCAN_SLEEP_SEC   = float(os.getenv("FULL_SCAN_SLEEP_SEC", "0.005")) # مدت مکث
FULL_SCAN_LOG_CHUNK   = int(os.getenv("FULL_SCAN_LOG_CHUNK", "100000"))  # هر چند خط یک لاگِ پیشرفت

# پیشرفت اسکن (برای UI)
FULL_SCAN_TOTAL_BYTES: int = 0
FULL_SCAN_READ_BYTES: int = 0
FULL_SCAN_DONE: bool = False
FULL_SCAN_ERR: Optional[str] = None





# -----------------------------
# لاگینگ
# -----------------------------
logger = logging.getLogger("darkob")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# -----------------------------
# پارس لاگ
# -----------------------------
# "YYYY-MM-DD HH:MM:SS | email | host"
RE_MATCHED = re.compile(
    r"^\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*([^|]+?)\s*\|\s*(.+)\s*$"
)

def parse_line(line: str) -> Optional[Dict[str, str]]:
    m = RE_MATCHED.match(line)
    if not m:
        return None
    ts, user, host = m.group(1), m.group(2).strip(), m.group(3).strip()
    if not user:
        return None
    return {"ts": ts, "user": user, "host": host or "UNKNOWN"}

def base_user(u: str) -> str:
    """برگرداندن base: حذف پیشوند عددی قبل از اولین نقطه (5823.mohammad -> mohammad)."""
    if not u:
        return u
    i = u.find(".")
    if i > 0:
        left, right = u[:i], u[i + 1 :]
        if left.isdigit():
            return right
    return u

# -----------------------------
# وضعیت حافظه‌ای
# -----------------------------
# رخدادهای اخیر (فقط برای live و عیب‌یابی‌های سبک)
recent_events: Deque[Dict[str, str]] = deque(maxlen=MAX_RECENT_EVENTS)

# شاخص کاربران: base -> aggregate
# aggregate: {
#   "count": int,
#   "last_ts": str,
#   "sample_host": str|None,
#   "variants": set(full-names),
#   "hosts": set(sample-of-hosts up to MAX_HOSTS_PER_USER)
# }
AGG: Dict[str, Dict[str, object]] = {}

class Broadcaster:
    def __init__(self) -> None:
        self.subscribers: Set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self.subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self.subscribers.discard(q)

    async def broadcast(self, msg: Dict[str, str]) -> None:
        if not self.subscribers:
            return
        dead = []
        for q in list(self.subscribers):
            try:
                q.put_nowait(msg)  # non-blocking
            except Exception:
                dead.append(q)
        for q in dead:
            self.unsubscribe(q)

broadcaster = Broadcaster()

# -----------------------------
# بارگذاری اولیه + دنبال‌کردن فایل
# -----------------------------
async def initial_index(path: str, scan_tail_lines: int = 50000) -> None:
    """
    به‌صورت سبک، فقط انتهای فایل را (مثلاً ۵۰هزار خط آخر) برای ساخت AGG می‌خوانیم
    تا UI از ابتدا اطلاعات داشته باشد؛ ولی همه‌ی تاریخچه را در حافظه نمی‌ریزیم.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()[-scan_tail_lines:]
        init = 0
        for ln in lines:
            ev = parse_line(ln.rstrip("\n"))
            if ev:
                _apply_event_to_agg(ev)
                init += 1
        logger.info(f"Initial index built from tail: {init} lines")
    except FileNotFoundError:
        logger.warning(f"File not found for initial index: {path}")
    except Exception as e:
        logger.exception(f"[initial_index] error: {e}")


async def full_scan_file(path: str) -> None:
    """
    کل فایل را از 0 تا اندازه‌ای که در لحظه‌ی شروع وجود دارد، آهسته می‌خواند
    و روی AGG اعمال می‌کند. هر چند خط مکث کوتاه تا فشار نیفتد.
    خطوطی که بعد از شروع اسکن اضافه می‌شوند را عمداً نمی‌خوانیم
    تا با tail_file دوباره‌شماری نشود.
    """
    global FULL_SCAN_TOTAL_BYTES, FULL_SCAN_READ_BYTES, FULL_SCAN_DONE, FULL_SCAN_ERR
    FULL_SCAN_DONE = False
    FULL_SCAN_ERR = None
    FULL_SCAN_TOTAL_BYTES = 0
    FULL_SCAN_READ_BYTES = 0

    try:
        st = os.stat(path)
        stop_offset = st.st_size               # نقطه‌ی توقف اسکن
        FULL_SCAN_TOTAL_BYTES = stop_offset

        applied = 0
        last_logged = 0

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            while True:
                pos_before = f.tell()
                if pos_before >= stop_offset:
                    break  # به اندازه‌ی تعیین‌شده رسیدیم؛ ادامه نمی‌دهیم

                line = f.readline()
                if not line:
                    # فایل کوتاه‌تر از stop_offset بوده ولی فعلاً داده‌ای نیست؛ کمی صبر کن
                    await asyncio.sleep(0.05)
                    continue

                FULL_SCAN_READ_BYTES = min(pos_before, stop_offset)

                ev = parse_line(line.rstrip("\n"))
                if ev:
                    _apply_event_to_agg(ev)
                    applied += 1
                    if applied % FULL_SCAN_SLEEP_EVERY == 0:
                        await asyncio.sleep(FULL_SCAN_SLEEP_SEC)

                if applied - last_logged >= FULL_SCAN_LOG_CHUNK:
                    last_logged = applied
                    logger.info(
                        f"[full-scan] applied: {applied}, progress: {FULL_SCAN_READ_BYTES}/{FULL_SCAN_TOTAL_BYTES} bytes"
                    )

        FULL_SCAN_READ_BYTES = FULL_SCAN_TOTAL_BYTES
        FULL_SCAN_DONE = True
        logger.info(f"[full-scan] completed. total applied: {applied}")

    except FileNotFoundError:
        FULL_SCAN_DONE = True
        FULL_SCAN_ERR = "log not found"
        logger.warning("[full-scan] file not found")
    except Exception as e:
        FULL_SCAN_DONE = True
        FULL_SCAN_ERR = f"{e}"
        logger.exception("[full-scan] error")





def _apply_event_to_agg(ev: Dict[str, str]) -> None:
    """به‌روزرسانی شاخص AGG با یک رخداد جدید."""
    b = base_user(ev["user"])
    a = AGG.get(b)
    if a is None:
        a = {
            "count": 0,
            "last_ts": ev["ts"],
            "sample_host": ev["host"],
            "variants": set([ev["user"]]),
            "hosts": set([ev["host"]]) if ev["host"] else set(),
        }
        AGG[b] = a
    # update
    a["count"] = int(a["count"]) + 1
    if not a.get("last_ts") or str(ev["ts"]) > str(a["last_ts"]):
        a["last_ts"] = ev["ts"]
        # نمونه‌ی host را از جدیدترین رخداد برداریم
        a["sample_host"] = ev["host"]
    # نام کامل را اضافه کن
    a["variants"].add(ev["user"])
    # می‌توانیم برای جست‌وجوی سایت لیستی نمونه‌ای از hostها نگه داریم
    hosts: Set[str] = a["hosts"]  # type: ignore
    if ev["host"] and len(hosts) < MAX_HOSTS_PER_USER:
        hosts.add(ev["host"])

async def tail_file(path: str) -> None:
    """فایل را دنبال می‌کند و رویدادهای جدید را به AGG و recent_events اضافه و برای SSE پخش می‌کند."""
    f = None
    inode = None
    first_open = True
    while True:
        try:
            if f is None:
                f = open(path, "r", encoding="utf-8", errors="ignore")
                st = os.fstat(f.fileno())
                inode = st.st_ino
                if first_open:
                    f.seek(0, os.SEEK_END)  # فقط خطوط جدید
                    first_open = False
                else:
                    f.seek(0, os.SEEK_SET)  # پس از rotation
                logger.info(f"Tailing file: {path} (inode={inode})")

            line = f.readline()
            if line:
                ev = parse_line(line.rstrip("\n"))
                if ev:
                    recent_events.append(ev)
                    _apply_event_to_agg(ev)
                    await broadcaster.broadcast(
                        {"ts": ev["ts"], "user": ev["user"], "base": base_user(ev["user"]), "host": ev["host"]}
                    )
            else:
                # بررسی rotation
                try:
                    cur_inode = os.stat(path).st_ino
                    if inode is not None and cur_inode != inode:
                        logger.info("Detected log rotation. Reopening file...")
                        try:
                            f.close()
                        except Exception:
                            pass
                        f = None
                        inode = None
                        continue
                except FileNotFoundError:
                    logger.warning("Log file temporarily missing; retrying...")
                    try:
                        f.close()
                    except Exception:
                        pass
                    f = None
                    inode = None
                    await asyncio.sleep(0.5)
                    continue

                await asyncio.sleep(0.2)
        except FileNotFoundError:
            logger.warning("Log file not found; waiting...")
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.exception(f"[tail_file] error: {e}")
            try:
                if f:
                    f.close()
            except Exception:
                pass
            f = None
            inode = None
            await asyncio.sleep(1.0)

# -----------------------------
# خواندن تاریخچه از فایل (paging)
# -----------------------------
def _iter_file_backward(path: str, chunk_size: int = 1 << 16):
    """ژنراتور خطوط فایل از انتها به ابتدا، بدون بارگذاری کل فایل در حافظه."""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        buffer = b""
        pos = f.tell()
        while pos > 0:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            data = f.read(read_size)
            buffer = data + buffer
            *lines, buffer = buffer.split(b"\n")
            for line in reversed(lines):
                yield line.decode("utf-8", errors="ignore")
        if buffer:
            # ابتدای فایل
            yield buffer.decode("utf-8", errors="ignore")

def tail_user_events(
    path: str,
    base: str,
    limit: int = 300,
    before_ts: Optional[str] = None,
) -> List[Dict[str, str]]:
    """
    آخرین رخدادهای مربوط به base را از انتهای فایل جمع می‌کند.
    اگر before_ts داده شود، فقط رخدادهایی که ts < before_ts هستند لحاظ می‌شوند.
    خروجی به ترتیب زمان صعودی بر می‌گردد.
    """
    results: List[Dict[str, str]] = []
    count = 0
    for raw in _iter_file_backward(path):
        ev = parse_line(raw.strip())
        if not ev:
            continue
        if before_ts and ev["ts"] >= before_ts:
            continue
        if base_user(ev["user"]) != base:
            continue
        results.append(ev)
        count += 1
        if count >= limit:
            break
    results.reverse()  # صعودی
    return results

# -----------------------------
# SSE
# -----------------------------
async def sse_event_generator(q: asyncio.Queue) -> AsyncGenerator[bytes, None]:
    try:
        yield b": ok\n\n"
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=SSE_HEARTBEAT_SEC)
                data = json.dumps(msg, ensure_ascii=False).encode("utf-8")
                yield b"event: message\n"
                yield b"data: " + data + b"\n\n"
            except asyncio.TimeoutError:
                yield b": ping\n\n"
    except asyncio.CancelledError:
        return

# -----------------------------
# وب‌اپ
# -----------------------------
app = FastAPI(title="Darkob — User Activity Panel (HP)", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# UI (مینیمال + دارک + دکمه‌های رتبه‌بندی)
# -----------------------------
INDEX_HTML = r"""<!doctype html>
<html lang="fa" dir="rtl" class="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Darkob — User Activity Monitor (HP)</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          colors: { base: '#0b0f14' },
          boxShadow: { glow: '0 0 0 1px rgba(56,189,248,.3)' }
        }
      }
    }
  </script>
  <script src="https://cdn.jsdelivr.net/npm/dayjs@1/dayjs.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/dayjs@1/plugin/utc.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/dayjs@1/plugin/timezone.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/jalaliday@2/dist/jalaliday.min.js"></script>
  <style>
    body { background: #0b0f14; }
    .card { background: rgba(255,255,255,.05); border: 1px solid rgba(255,255,255,.08); }
    .chip { background: rgba(255,255,255,.08); border: 1px solid rgba(255,255,255,.10); }
    .input { background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.10); }
    .btn { background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.10); }
    .btn:hover { background: rgba(255,255,255,.10); }
    .chip-amber { background: rgba(245, 158, 11, .15); border: 1px solid rgba(245, 158, 11, .35); color: rgb(251 191 36); }
    .chip-red   { background: rgba(239, 68, 68, .15); border: 1px solid rgba(239, 68, 68, .35); color: rgb(248 113 113); }
    .btn-amber { background: rgba(245, 158, 11, .15); border: 1px solid rgba(245, 158, 11, .35); color: rgb(251 191 36); }
    .btn-red   { background: rgba(239, 68, 68, .15); border: 1px solid rgba(239, 68, 68, .35); color: rgb(248 113 113); }
  </style>
</head>
<body class="text-slate-100">
<header class="sticky top-0 z-20 backdrop-blur bg-black/30 border-b border-white/5">
  <div class="max-w-7xl mx-auto px-4 py-4 flex items_center justify_between">
    <div class="flex items-center gap-3">
      <div class="w-9 h-9 rounded-2xl bg-gradient-to-br from-indigo-500 to-sky-400"></div>
      <h1 class="text-xl font-semibold tracking-tight">Darkob — User Activity Monitor (HP)</h1>
      <span class="text-xs px-2 py-0.5 rounded-full chip uppercase">High Performance</span>
    </div>
    <div class="flex items-center gap-2">
      <button id="btn-export-all" class="px-3 py-1.5 rounded-xl btn text-sm">خروجی CSV snapshot</button>
      <button id="btn-list-warn" class="px-3 py-1.5 rounded-xl btn-amber text-sm">⚠️ رتبه‌بندی اخطار</button>
      <button id="btn-list-deac" class="px-3 py-1.5 rounded-xl btn-red text-sm">⛔ رتبه‌بندی غیرفعال</button>
      <button id="btn-pause" class="px-3 py-1.5 rounded-xl btn text-sm">⏸️ توقف زنده</button>
      <span id="scan-ind" class="text-xs px-2 py-0.5 rounded-full chip">اسکن اولیه: …</span>

    </div>
  </div>
</header>

<section class="max-w-7xl mx-auto px-4 mt-6">
  <div class="grid grid-cols-1 lg:grid-cols-12 gap-3">
    <div class="lg:col-span-4">
      <input id="search-user" class="w-full px-3 py-2 rounded-xl input outline-none focus:ring-2 focus:ring-sky-500" placeholder="جستجو براساس نام کاربری (base یا کامل)">
    </div>
    <div class="lg:col-span-4">
      <input id="search-site" class="w-full px-3 py-2 rounded-xl input outline-none focus:ring-2 focus:ring-sky-500" placeholder="جستجو براساس نام سایت (نمونه‌ها)">
    </div>
    <div class="lg:col-span-4 text-sm opacity-80 flex items-center gap-2">
      <span>خواندن از <span class="px-2 py-0.5 chip rounded">connections_log.txt</span> بدون حذف لاگ‌ها</span>
      <span id="live-dot" class="w-2 h-2 rounded-full bg-green-400 inline-block"></span>
      

    </div>
  </div>
</section>

<section class="max-w-7xl mx-auto px-4 mt-6">
  <div class="grid grid-cols-3 gap-3 text-sm">
    <div class="rounded-2xl p-4 card">
      <div class="opacity-70">تعداد کاربران (base)</div>
      <div id="stat-users" class="text-2xl font-semibold">0</div>
    </div>
    <div class="rounded-2xl p-4 card">
      <div class="opacity-70">رخدادهای اخیر (حافظه)</div>
      <div id="stat-logs" class="text-2xl font-semibold">0</div>
    </div>
    <div class="rounded-2xl p-4 card">
      <div class="opacity-70">آخرین بروزرسانی</div>
      <div id="stat-last" class="text-2xl font-semibold">—</div>
    </div>
  </div>
</section>

<section class="max-w-7xl mx_auto px-4 mt-6 pb-24">
  <div id="users-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4"></div>
  <div id="empty" class="text-center opacity-70 py-16 hidden">کاربری پیدا نشد.</div>
</section>

<!-- Per-Base Modal -->
<div id="modal" class="fixed inset-0 z-30 hidden items-end sm:items-center justify-center bg-black/50">
  <div class="w-full sm:w-[900px] max_h-[90vh] rounded-t-2xl sm:rounded-2xl overflow-hidden card">
    <div class="px-4 py-3 border-b border-white/10">
      <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div class="font-semibold" id="modal-title">گزارش — </div>
        <div class="flex items-center gap-2">
          <input id="modal-search" class="w-64 px-3 py-1.5 rounded-xl input outline-none focus:ring-2 focus:ring-sky-500" placeholder="فیلتر سایت (در این لیست)">
          <button id="btn-load-more" class="px-3 py-1.5 rounded-xl btn text-sm hidden">بیشتر...</button>

          <button id="btn-close" class="px-3 py-1.5 rounded-xl border border-white/10">✕</button>
        </div>
      </div>
    </div>
    <div id="modal-body" class="p-4 space-y-2 overflow-auto" style="max-height: 72vh"></div>
  </div>
</div>

<!-- Stats Modal -->
<div id="modal-stats" class="fixed inset-0 z-40 hidden items-end sm:items-center justify-center bg-black/50">
  <div class="w-full sm:w-[900px] max_h-[90vh] rounded-t-2xl sm:rounded-2xl overflow-hidden card">
    <div class="px-4 py-3 border-b border-white/10 flex items-center justify-between">
      <div class="font-semibold" id="modal-stats-title">رتبه‌بندی کاربران</div>
      <div class="flex items-center gap-2">
        <button id="btn-export-stats" class="px-3 py-1.5 rounded-xl btn text-sm">خروجی CSV</button>
        <button id="btn-close-stats" class="px-3 py-1.5 rounded-xl border border-white/10">✕</button>
      </div>
    </div>
    <div id="modal-stats-body" class="p-4 space-y-2 overflow-auto" style="max-height: 72vh"></div>
  </div>
</div>

<script>
dayjs.extend(dayjs_plugin_utc);
dayjs.extend(dayjs_plugin_timezone);
dayjs.extend(jalaliday);
try { dayjs.calendar('jalali'); } catch(e) {}


const tz = 'Asia/Tehran';
function toJalaliTehran(ts){
  try{
    const d = dayjs.tz(ts, tz);
    const j = d.calendar ? d.calendar('jalali') : d;
    return j.format('YYYY-MM-DD HH:mm:ss');
  }catch(e){ return ts; }
}

// ---------- State ----------
let usersMap = new Map(); // base -> {base, display_names[], count, last_ts, sample_host, warn, deac, hosts[]}
let statsMode = null;     // 'warn' | 'deac' | null
let modalBase = null;
let modalOldestTs = null; // cursor for paging older
let modalAll = [];        // current list in modal
let livePaused = false;
const liveDot = document.getElementById('live-dot');

// ---------- DOM ----------
const gridEl = document.getElementById('users-grid');
const emptyEl = document.getElementById('empty');
const statUsers = document.getElementById('stat-users');
const statLogs  = document.getElementById('stat-logs');
const statLast  = document.getElementById('stat-last');
const PAGE_SIZE = 80000; // یا 5000، مطابق سقف سرور

let cancelModalLoad = false; // برای قطع‌کردن لود وقتی مودال بسته شد

function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
function pickSampleHost(hosts, currentSample){
  if (Array.isArray(hosts)) {
    const hit = hosts.find(h => (h||'').toLowerCase().includes('porn'));
    if (hit) return hit;
    if (currentSample && hosts.includes(currentSample)) return currentSample;
    return hosts[0] || currentSample || '';
  }
  return currentSample || '';
}

const searchUserEl = document.getElementById('search-user');
const searchSiteEl = document.getElementById('search-site');

const btnExportAll = document.getElementById('btn-export-all');
const btnPause = document.getElementById('btn-pause');
const btnListWarn = document.getElementById('btn-list-warn');
const btnListDeac = document.getElementById('btn-list-deac');

const modal = document.getElementById('modal');
const modalTitle = document.getElementById('modal-title');
const modalBody = document.getElementById('modal-body');
const modalSearchEl = document.getElementById('modal-search');
const btnClose = document.getElementById('btn-close');
const btnLoadMore = document.getElementById('btn-load-more');

const modalStats = document.getElementById('modal-stats');
const modalStatsTitle = document.getElementById('modal-stats-title');
const modalStatsBody = document.getElementById('modal-stats-body');
const btnCloseStats = document.getElementById('btn-close-stats');
const btnExportStats = document.getElementById('btn-export-stats');
const scanInd = document.getElementById('scan-ind');




document.addEventListener('keydown', (e)=>{
  if(e.key==='Escape'){ hideModal(); hideStatsModal(); }
});

function showModal(){ modal.classList.remove('hidden'); modal.classList.add('flex'); }


function showStatsModal(){ modalStats.classList.remove('hidden'); modalStats.classList.add('flex'); }
function hideStatsModal(){ modalStats.classList.add('hidden'); modalStats.classList.remove('flex'); statsMode=null; }
btnCloseStats.onclick = hideStatsModal;

// ---------- API Helpers ----------

function isPornHost(h){
  if(!h) return false;
  return String(h).toLowerCase().includes('porn');
}


async function preloadAllForModal(){
  if(!modalBase) return;
  const baseAtOpen = modalBase;          // محافظ تعویض یوزر
  let pages = 0;
  let lastCursor = null;

  while(!cancelModalLoad && baseAtOpen === modalBase){
    const cursorBefore = modalOldestTs;  // کِرسور قبلی برای تشخیص گیر
    const page = await fetchUserEvents(baseAtOpen, PAGE_SIZE, cursorBefore);

    if(cancelModalLoad || baseAtOpen !== modalBase) return;

    if(!page.length){
      // اگر خالی برگشت ولی هنوز cursor عوض نشده، تلاش آخر با کمی تأخیر
      if (cursorBefore && lastCursor !== '__retry__') {
        lastCursor = '__retry__';
        await sleep(60);
        continue;
      }
      break;
    }

    modalAll = modalAll.concat(page);

    // توجه: سرور نتایج را «صعودی» برمی‌گرداند => قدیمی‌ترینِ این صفحه index صفر است
    const newCursor = page[0]?.ts || cursorBefore;

    // اگر cursor تغییری نکرد، یعنی به تهٔ بازه رسیدیم یا تایم‌استمپ‌ها برابرند
    if (newCursor === cursorBefore) break;

    modalOldestTs = newCursor;
    lastCursor = modalOldestTs;
    renderModal();

    if(++pages % 5 === 0) await sleep(40); // نفس برای UI
  }
}


async function fetchUsersSnapshot(){
  const u = encodeURIComponent(searchUserEl.value||'');
  const s = encodeURIComponent(searchSiteEl.value||'');
  const resp = await fetch(`/api/users?user_q=${u}&site_q=${s}&limit=600`);
  const data = await resp.json();
  // rebuild map
  usersMap.clear();
  for(const it of data){
    usersMap.set(it.base, it);
  }
  statUsers.textContent = String(usersMap.size);
  statLogs.textContent = '≈' + String(5000); // نشان‌دهنده‌ی حافظه‌ی زنده؛ فایل حذف نمی‌شود
  const last = data.length ? data[0].last_ts : null;
  statLast.textContent = last ? toJalaliTehran(last) : '—';
}

async function pollScan(){
  try{
    const r = await fetch('/api/scan_progress');
    const j = await r.json();
    if(j.error){
      scanInd.textContent = 'اسکن: خطا';
    }else if(j.done){
      scanInd.textContent = 'اسکن: کامل';
    }else{
      scanInd.textContent = `اسکن: ${j.percent.toFixed(1)}%`;
    }
  }catch(e){
    // ignore
  }
}
setInterval(pollScan, 1000);
pollScan();


async function fetchUserEvents(base, limit=300, beforeTs=null){
  const url = new URL(location.origin + '/api/user_events');
  url.searchParams.set('base', base);
  url.searchParams.set('limit', limit);
  if(beforeTs) url.searchParams.set('before_ts', beforeTs);
  const resp = await fetch(url);
  return await resp.json();
}

async function refreshStats(){
  try{
    const resp = await fetch('/api/stats');
    const raw = await resp.json();
    // normalize to base
    const norm = {};
    for(const k in raw){
      const i = k.indexOf('.');
      let b = k;
      if(i>0 && /^[0-9]+$/.test(k.slice(0,i))) b = k.slice(i+1);
      if(!norm[b]) norm[b] = {warnings_after_second:0, deactivated_times:0};
      const v = raw[k] || {};
      norm[b].warnings_after_second += Number(v.warnings_after_second||0);
      norm[b].deactivated_times     += Number(v.deactivated_times||0);
    }
    // merge into usersMap
    for(const [b, it] of usersMap.entries()){
      const st = norm[b] || {warnings_after_second:0, deactivated_times:0};
      it.warn = st.warnings_after_second || 0;
      it.deac = st.deactivated_times || 0;
      usersMap.set(b, it);
    }
    // if stats modal open, rerender
    if(statsMode) renderStats();
  }catch(e){}
}

// ---------- Rendering ----------
function renderGrid(){
  const qUser = (searchUserEl.value||'').toLowerCase().trim();
  const qSite = (searchSiteEl.value||'').toLowerCase().trim();

  let arr = Array.from(usersMap.values());
  if(qUser){
    arr = arr.filter(x => {
      if((x.base||'').toLowerCase().includes(qUser)) return true;
      return x.display_names.some(d => (d||'').toLowerCase().includes(qUser));
    });
  }
  if(qSite){
    arr = arr.filter(x => (x.hosts||[]).some(h => (h||'').toLowerCase().includes(qSite)));
  }
  arr.sort((a,b)=> (b.last_ts||'').localeCompare(a.last_ts||''));

  const MAX_CARDS = 500;
  const sliced = arr.slice(0, MAX_CARDS);
  gridEl.innerHTML='';
  if(!sliced.length){ emptyEl.classList.remove('hidden'); return; }
  emptyEl.classList.add('hidden');

  const frag = document.createDocumentFragment();
  for(const it of sliced){
    const warn = Number(it.warn||0);
    const deac = Number(it.deac||0);
    const card = document.createElement('button');
    card.className = 'group text-right rounded-2xl p-4 card hover:shadow-glow transition';
    card.innerHTML = `
      <div class="flex items-center justify-between gap-2">
        <div class="font-semibold truncate max-w-[60%]">${it.base}</div>
        <div class="flex items-center gap-1">
          <span class="text-xs px-2 py-0.5 rounded-full chip">${it.count}</span>
          <span class="text-xs px-2 py-0.5 rounded-full chip-amber">⚠️ ${warn}</span>
          <span class="text-xs px-2 py-0.5 rounded-full chip-red">⛔ ${deac}</span>
        </div>
      </div>
      <div class="mt-1 text-xs opacity-70 truncate">${(it.display_names||[]).join('، ')}</div>
      <div class="mt-2 text-sm opacity-80 truncate">نمونه سایت: ${it.sample_host || '—'}</div>
      <div class="mt-2 text-xs opacity-60">آخرین فعالیت: ${it.last_ts ? toJalaliTehran(it.last_ts) : '—'}</div>
    `;
    card.onclick = ()=> openBase(it.base);
    frag.appendChild(card);
  }
  gridEl.appendChild(frag);
}

function showStats(mode){
  statsMode = mode;
  renderStats();
  showStatsModal();
}

function renderStats(){
  const rows = [];
  for(const it of usersMap.values()){
    const warn = Number(it.warn||0);
    const deac = Number(it.deac||0);
    rows.push({ base: it.base, displayNames: it.display_names||[], warn, deac });
  }
  rows.sort((a,b)=>{
    if(statsMode==='warn'){
      if(b.warn!==a.warn) return b.warn - a.warn;
      if(b.deac!==a.deac) return b.deac - a.deac;
    }else{
      if(b.deac!==a.deac) return b.deac - a.deac;
      if(b.warn!==a.warn) return b.warn - a.warn;
    }
    return a.base.localeCompare(b.base);
  });

  modalStatsTitle.innerText = statsMode==='warn'
    ? 'رتبه‌بندی کاربران بر اساس ⚠️ اخطار (از دوم به بعد)'
    : 'رتبه‌بندی کاربران بر اساس ⛔ غیرفعال‌شدن';

  const frag = document.createDocumentFragment();
  let rank = 1;
  for(const r of rows){
    if(statsMode==='warn' && r.warn<=0) continue;
    if(statsMode==='deac' && r.deac<=0) continue;
    const item = document.createElement('button');
    item.className = 'w-full text-right rounded-2xl border border-white/10 bg-white/5 px-3 py-2 hover:shadow-glow transition';
    item.innerHTML = `
      <div class="flex items-center justify-between gap-2">
        <div class="truncate">
          <div class="text-sm font-semibold truncate">#${rank} — ${r.base}</div>
          <div class="text-xs opacity-70 truncate">${r.displayNames.join('، ')}</div>
        </div>
        <div class="flex items-center gap-1">
          <span class="text-xs px-2 py-0.5 rounded-full chip-amber">⚠️ ${r.warn}</span>
          <span class="text-xs px-2 py-0.5 rounded-full chip-red">⛔ ${r.deac}</span>
        </div>
      </div>
    `;
    item.onclick = ()=> openBase(r.base);
    frag.appendChild(item);
    rank++;
  }
  modalStatsBody.innerHTML = '';
  if(!frag.childNodes.length){
    modalStatsBody.innerHTML = '<div class="opacity-70 text-sm text-center py-6">موردی برای نمایش وجود ندارد.</div>';
  } else {
    modalStatsBody.appendChild(frag);
  }
}

// ---------- Modal ----------
async function openBase(base){
  modalBase = base;
  modalAll = [];
  modalOldestTs = null;
  cancelModalLoad = false;

  showModal();
  renderModal();                // حتی با لیست خالی، تا سرچ از همون لحظه آماده باشد
  preloadAllForModal().catch(()=>{});
}


function hideModal(){
  modal.classList.add('hidden');
  modal.classList.remove('flex');
  cancelModalLoad = true;
  modalBase = null;
  modalAll = [];
  modalOldestTs = null;
  modalSearchEl.value = '';
}


async function loadMore(){
  if(!modalBase) return;
  const page = await fetchUserEvents(modalBase, PAGE_SIZE, modalOldestTs);
  if(page.length){
    modalAll = modalAll.concat(page);
    modalOldestTs = page[0].ts;
  }
}


function renderModal(){
  const q = (modalSearchEl.value||'').toLowerCase().trim();
  const rows = modalAll.filter(r => !q || (r.host||'').toLowerCase().includes(q));

  // اول پورن‌ها، بعد بقیه
  const pornRows = [], otherRows = [];
  for (const r of rows){
    (isPornHost(r.host) ? pornRows : otherRows).push(r);
  }
  const ordered = pornRows.concat(otherRows);

  modalTitle.innerText = `گزارش — ${modalBase} (نمایش ${ordered.length} از ${modalAll.length})`;

  const frag = document.createDocumentFragment();
  for(const r of ordered){
    const row = document.createElement('div');
    row.className = 'flex items-center justify-between rounded-2xl border border-white/10 bg-white/5 px-3 py-2';
    row.innerHTML = `
      <div class="truncate">
        <div class="text-sm font_medium truncate">${r.host}</div>
        <div class="text-xs opacity-70">${toJalaliTehran(r.ts)} — <span class="opacity-80">${r.user}</span></div>
      </div>
    `;
    frag.appendChild(row);
  }
  modalBody.innerHTML='';
  modalBody.appendChild(frag);
} // ← این } حیاتی بود

btnLoadMore.onclick = async ()=>{ await loadMore(); renderModal(); };

let modalSearchTimer = null;
modalSearchEl.addEventListener('input', ()=>{
  clearTimeout(modalSearchTimer);
  modalSearchTimer = setTimeout(()=> renderModal(), 120);
});



// ---------- Controls ----------
btnExportAll.onclick = async ()=>{
  const arr = Array.from(usersMap.values()).sort((a,b)=> (b.last_ts||'').localeCompare(a.last_ts||''));
  const header = 'base,display_names,count,last_ts,sample_host,warn,deac\n';
  const body = arr.map(it => [
    JSON.stringify(it.base),
    JSON.stringify((it.display_names||[]).join(' | ')),
    it.count,
    it.last_ts,
    JSON.stringify(it.sample_host||''),
    it.warn||0,
    it.deac||0
  ].join(',')).join('\n');
  const blob = new Blob([header + body], {type:'text/csv;charset=utf-8;'});
  const url = URL.createObjectURL(blob); const a = document.createElement('a');
  a.href = url; a.download = 'users_snapshot.csv'; a.click(); URL.revokeObjectURL(url);
};

btnPause.onclick = ()=>{
  livePaused = !livePaused;
  btnPause.textContent = livePaused ? '▶️ ادامه زنده' : '⏸️ توقف زنده';
  liveDot.style.backgroundColor = livePaused ? '#f59e0b' : '#34d399';
};

btnListWarn.onclick = ()=> showStats('warn');
btnListDeac.onclick = ()=> showStats('deac');

// ---------- Live (SSE) ----------
let es = null;
let dirtyCount = 0;
let tickScheduled = false;
function scheduleTick(){
  if(tickScheduled) return;
  tickScheduled = true;
  setTimeout(()=>{
    tickScheduled = false;
    if(!livePaused && dirtyCount>0){
      renderGrid();
      dirtyCount = 0;
    }
  }, 300); // batch UI updates every 300ms
}
function startLive(){
  es = new EventSource('/api/events');
  es.onmessage = (ev)=>{
    try{
      if(livePaused) return;
      const data = JSON.parse(ev.data);
      const base = data.base;
      let it = usersMap.get(base);
      if(!it){
        it = {base: base, display_names: [data.user], count: 1, last_ts: data.ts, sample_host: data.host, warn: 0, deac: 0, hosts: [data.host]};
      }else{
        it.count = (it.count||0) + 1;
        if(!it.last_ts || data.ts > it.last_ts) {
          it.last_ts = data.ts;
          it.sample_host = data.host;
        }
        if(!it.display_names.includes(data.user)) it.display_names.push(data.user);
        const hs = new Set(it.hosts||[]);
        if(hs.size < 200){ hs.add(data.host); }
        it.hosts = Array.from(hs.values());        // ← بیرون if بگذار که همیشه ست شود
        it.sample_host = pickSampleHost(it.hosts, it.sample_host);
        

      }
      usersMap.set(base, it);
      dirtyCount++;
      scheduleTick();
    }catch(e){}
  };
  es.onerror = ()=>{};
}

// ---------- Filters ----------
let inputTimer = null;
function onInputChanged(){
  clearTimeout(inputTimer);
  inputTimer = setTimeout(()=> renderGrid(), 150);
}
searchUserEl.addEventListener('input', onInputChanged);
searchSiteEl.addEventListener('input', onInputChanged);

// ---------- Bootstrap ----------
async function bootstrap(){
  await fetchUsersSnapshot();
  await refreshStats();
  renderGrid();
  startLive();
  setInterval(async ()=>{
    await fetchUsersSnapshot();
    await refreshStats();
    renderGrid();
  }, 10000);
}
bootstrap();
</script>
</body>
</html>
"""

# -----------------------------
# API ها
# -----------------------------
@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(INDEX_HTML)

@app.get("/api/health", response_class=PlainTextResponse)
async def health():
    return "ok"

@app.get("/api/events")
async def api_events(request: Request):
    q = broadcaster.subscribe()
    async def gen():
        try:
            async for chunk in sse_event_generator(q):
                if await request.is_disconnected():
                    break
                yield chunk
        finally:
            broadcaster.unsubscribe(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/users")
async def api_users(
    user_q: str = Query(default=""),
    site_q: str = Query(default=""),
    limit: int = Query(default=600, ge=1, le=5000),
):
    """
    Snapshot سبک از کاربران (base) بر اساس AGG در حافظه.
    جست‌وجوی نام کاربری و سایت روی نمونه hostها انجام می‌شود.
    """
    uq = (user_q or "").strip().lower()
    sq = (site_q or "").strip().lower()

    # ساخت لیست از AGG
    items = []
    for b, a in AGG.items():
        display = sorted(a["variants"]) if a.get("variants") else [b]
        hosts_set = a.get("hosts", set())
        hosts = list(hosts_set)[:MAX_HOSTS_PER_USER]
        prio = next((h for h in hosts if h and 'porn' in h.lower()), None)
        sample = prio or str(a.get("sample_host", ""))
    
        items.append({  # ✅ همين يك append کافيست
            "base": b,
            "display_names": display,
            "count": int(a.get("count", 0)),
            "last_ts": str(a.get("last_ts", "")),
            "sample_host": sample,
            "hosts": hosts,
        })


    # فیلترها
    if uq:
        items = [
            it for it in items
            if (it["base"].lower().find(uq) != -1)
            or any((d.lower().find(uq) != -1) for d in it["display_names"])
        ]
    if sq:
        items = [
            it for it in items
            if any(((h or "").lower().find(sq) != -1) for h in it["hosts"])
        ]

    # مرتب‌سازی و محدودسازی
    items.sort(key=lambda x: x.get("last_ts", ""), reverse=True)
    return JSONResponse(items[:limit])

@app.get("/api/user_events")
async def api_user_events(
    base: str,
    limit: int = Query(default=3000, ge=10, le=10000),  # ← سقف واقعی
    before_ts: Optional[str] = None,
):
    try:
        data = tail_user_events(LOG_PATH, base_user(base), limit=limit, before_ts=before_ts)
        return JSONResponse(data)
    except FileNotFoundError:
        return JSONResponse([], status_code=200)

@app.get("/api/stats")
async def api_stats():
    try:
        if not os.path.exists(USER_STATS_FILE):
            return JSONResponse({})
        with open(USER_STATS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if not isinstance(data, dict):
                data = {}
        return JSONResponse(data)
    except Exception as e:
        logger.exception("Failed to read stats: %s", e)
     
        return JSONResponse({})


@app.get("/api/scan_progress")
async def api_scan_progress():
    try:
        total = FULL_SCAN_TOTAL_BYTES or 0
        readb = FULL_SCAN_READ_BYTES or 0
        done  = bool(FULL_SCAN_DONE)
        err   = FULL_SCAN_ERR
        pct = 0.0
        if total > 0:
            pct = min(100.0, (readb / total) * 100.0)
        return JSONResponse({
            "total_bytes": total,
            "read_bytes": readb,
            "percent": pct,
            "done": done,
            "error": err
        })
    except Exception:
        return JSONResponse({"total_bytes":0,"read_bytes":0,"percent":0,"done":False,"error":"internal"}, status_code=200)



# -----------------------------
# startup
# -----------------------------
@app.on_event("startup")
async def on_start():
    # فقط دنبال کردن خطوط جدید (بدون معطّل‌کردن UI)
    asyncio.create_task(tail_file(LOG_PATH))
    # اسکن کاملِ آهسته در پس‌زمینه
    asyncio.create_task(full_scan_file(LOG_PATH))
    logger.info(f"Startup scheduled. Tailing: {LOG_PATH}; Full-scan scheduled; Stats: {USER_STATS_FILE}")


def main():
    global LOG_PATH
    parser = argparse.ArgumentParser(description="Darkob Live Panel (HP)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=8888, type=int)
    parser.add_argument("--log-level", default="info", choices=["debug","info","warning","error"])
    parser.add_argument("--log-path", default=LOG_PATH)
    args = parser.parse_args()

    LOG_PATH = args.log_path
    logger.setLevel(getattr(logging, args.log_level.upper(), logging.INFO))

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)

if __name__ == "__main__":
    main()
