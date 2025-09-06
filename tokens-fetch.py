import re
import asyncio
from playwright.async_api import async_playwright
from aiofiles import open as aio_open
from datetime import datetime
import random
import socket
from typing import Optional

TOKEN_FILE = "token.txt"
DESIRED_TOKENS = 25
CONCURRENT_TASKS = 3  # try 3–4; higher risks getting blocked
NAV_TIMEOUT = 30000   # ms; faster than 60000
TOKEN_WAIT = 15000    # ms; how long we wait for a token event before retrying

URL = "https://lis.pulse.gop.pk"
TOKEN_RE = re.compile(r"token=([A-Za-z0-9\-_]+)")

# ---------------- utils ----------------
def check_internet(host="8.8.8.8", port=53, timeout=2):
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except Exception:
        return False

async def file_writer(queue: asyncio.Queue):
    """Single writer task to reduce file open/close overhead."""
    async with aio_open(TOKEN_FILE, "a", encoding="utf-8") as f:
        while True:
            item = await queue.get()
            if item is None:
                break
            await f.write(item + "\n")
            queue.task_done()

# ------------- core harvester -------------
async def grab_one_token(browser) -> Optional[str]:
    """
    Create a fresh context/page, navigate quickly, trigger map traffic,
    wait for first MapServer request/response containing token=..., return token.
    """
    context = await browser.new_context(ignore_https_errors=True,
                                       user_agent=f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                                  f"AppleWebKit/537.36 (KHTML, like Gecko) "
                                                  f"Chrome/{random.randint(117,125)}.0.0.0 Safari/537.36")
    page = await context.new_page()
    token_holder = {"token": None}

    def maybe_take(url: str):
        if "VendorMaps/Punjab_Cdastral_Maps" in url and "MapServer" in url and "token=" in url:
            m = TOKEN_RE.search(url)
            if m:
                token_holder["token"] = m.group(1)

    page.on("request", lambda req: maybe_take(req.url))
    page.on("response", lambda resp: maybe_take(resp.url))

    try:
        # Fast nav: domcontentloaded is enough to start app scripts
        await page.goto(URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)

        # Nudge page so the app fires network calls
        try:
            await page.mouse.move(random.randint(100, 500), random.randint(120, 520))
            await page.mouse.wheel(0, random.randint(150, 350))
        except Exception:
            pass

        # Wait until token appears or timeout
        # We implement a simple polling with a short sleep to avoid event races
        deadline = asyncio.get_event_loop().time() + (TOKEN_WAIT / 1000)
        while token_holder["token"] is None and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.15)

        return token_holder["token"]

    except Exception as e:
        print(f"[!] {datetime.now()} navigation/token error: {e}")
        return None
    finally:
        try:
            await context.close()
        except Exception:
            pass

async def worker(browser, out_queue: asyncio.Queue, unique: set, idx: int):
    """Repeatedly harvest tokens until the global target is reached."""
    failures = 0
    while len(unique) < DESIRED_TOKENS:
        if not check_internet():
            print(f"[W{idx}] [!] {datetime.now()} No internet, sleeping 6s...")
            await asyncio.sleep(6)
            continue

        token = await grab_one_token(browser)
        if token:
            if token not in unique:
                unique.add(token)
                await out_queue.put(token)
                print(f"[W{idx}] [+] Token {len(unique)}/{DESIRED_TOKENS}: {token[:12]}...")
                failures = 0
            else:
                print(f"[W{idx}] [=] Duplicate skipped.")
        else:
            failures += 1
            print(f"[W{idx}] [!] No token this round.")
            # brief backoff after a couple of misses
            if failures >= 2:
                await asyncio.sleep(random.uniform(4, 7))
                failures = 0

async def collect_tokens_fast():
    # Overwrite the file at the start
    async with aio_open(TOKEN_FILE, "w", encoding="utf-8") as f:
        await f.write("")

    if not check_internet():
        print(f"[!] {datetime.now()} No internet detected. Continuing anyway...")

    unique_tokens = set()
    out_queue = asyncio.Queue()
    writer_task = asyncio.create_task(file_writer(out_queue))

    async with async_playwright() as pw:
        # Launch ONE browser; contexts are cheap and fast
        browser = await pw.chromium.launch(headless=True)

        print(f"⚡ Collecting {DESIRED_TOKENS} tokens with {CONCURRENT_TASKS} workers...")
        workers = [
            asyncio.create_task(worker(browser, out_queue, unique_tokens, i+1))
            for i in range(CONCURRENT_TASKS)
        ]

        await asyncio.gather(*workers)

        # flush writer
        await out_queue.put(None)
        await writer_task

    print(f"\n✅ Done. {len(unique_tokens)} token(s) saved to: {TOKEN_FILE}")

# Entry
if __name__ == "__main__":
    asyncio.run(collect_tokens_fast())
