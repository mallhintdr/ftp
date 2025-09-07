# sniff_lis_token_txt.py  — writes token,proxy_url (with creds if present)
import asyncio
import re
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Set
from urllib.parse import quote

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

URL = "https://lis.pulse.gop.pk"
TOKEN_FILE = "token.txt"     # token,proxy_url
PROXY_FILE = "PROXYLIST.txt"

NAV_TIMEOUT_MS = 60000
TOKEN_WAIT_MS  = 20000
MAX_CONCURRENCY = 8
DESIRED_TOKENS  = 0          # 0 = try all proxies; set N to stop after N tokens
RETRIES_PER_PROXY = 1

TOKEN_RE = re.compile(r"[?&]token=([A-Za-z0-9\-_]+)")

def parse_proxy_line(line: str) -> Optional[Tuple[str, Dict]]:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None
    parts = raw.split(":")
    if len(parts) == 2:
        host, port = parts
        server = f"http://{host}:{port}"
        return raw, {"server": server, "host": host, "port": port, "username": None, "password": None}
    if len(parts) == 4:
        host, port, user, pwd = parts
        server = f"http://{host}:{port}"
        return raw, {"server": server, "host": host, "port": port, "username": user, "password": pwd}
    return None

def load_proxies(path=PROXY_FILE) -> List[Dict]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    parsed = [p[1] for ln in lines if (p := parse_proxy_line(ln))]
    if not parsed:
        raise RuntimeError("No proxies parsed from PROXYLIST.txt")
    return parsed

def build_proxy_url(px: Dict) -> str:
    """
    Returns a proxy URL suitable for requests/HTTP CONNECT:
      - with creds:  http://user:pass@host:port
      - no creds:    http://host:port
    Username/password are URL-encoded.
    """
    if px.get("username"):
        u = quote(px["username"], safe="")
        p = quote(px.get("password") or "", safe="")
        return f"http://{u}:{p}@{px['host']}:{px['port']}"
    return f"http://{px['host']}:{px['port']}"

async def grab_token_via_proxy(browser, px: Dict) -> Optional[str]:
    ctx_args = {
        "ignore_https_errors": True,
        "proxy": {"server": px["server"]} if not px["username"] else
                 {"server": px["server"], "username": px["username"], "password": px["password"]},
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    context = await browser.new_context(**ctx_args)
    context.set_default_navigation_timeout(NAV_TIMEOUT_MS)

    async def route_filter(route):
        if route.request.resource_type in ("image", "font", "media"):
            return await route.abort()
        return await route.continue_()
    await context.route("**/*", route_filter)

    page = await context.new_page()
    token_holder = {"token": None}

    def maybe_take(url: str):
        if "gismaps.pulse.gop.pk/arcgis/rest/services/VendorMaps/Punjab_Cdastral_Maps/MapServer" in url:
            m = TOKEN_RE.search(url)
            if m:
                token_holder["token"] = m.group(1)

    page.on("request",  lambda req: maybe_take(req.url))
    page.on("response", lambda resp: maybe_take(resp.url))

    try:
        try:
            await page.goto(URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        except PWTimeout:
            return None

        await page.mouse.move(220, 220)
        await page.mouse.wheel(0, 180)

        deadline = asyncio.get_running_loop().time() + (TOKEN_WAIT_MS / 1000)
        while token_holder["token"] is None and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.15)

        return token_holder["token"]
    finally:
        try:
            await context.close()
        except Exception:
            pass

async def main():
    proxies = load_proxies()

    # Clear token.txt at start (avoid stale/expired tokens)
    Path(TOKEN_FILE).write_text("", encoding="utf-8")

    minted_tokens: Set[str] = set()
    minted_count = 0

    queue = asyncio.Queue()
    done_event = asyncio.Event()

    async def writer():
        nonlocal minted_count
        while not (done_event.is_set() and queue.empty()):
            try:
                token, px = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if token not in minted_tokens:
                minted_tokens.add(token)
                minted_count += 1
                proxy_url = build_proxy_url(px)  # <-- include creds if present
                with open(TOKEN_FILE, "a", encoding="utf-8") as f:
                    f.write(f"{token},{proxy_url}\n")
                print(f"  ✔ token #{minted_count}: {token[:10]}… via {proxy_url}")
            queue.task_done()

    async def proxy_worker(px: Dict, sem: asyncio.Semaphore):
        nonlocal minted_count
        if DESIRED_TOKENS and minted_count >= DESIRED_TOKENS:
            return
        async with sem:
            attempts = 1 + max(0, RETRIES_PER_PROXY)
            for _ in range(attempts):
                if DESIRED_TOKENS and minted_count >= DESIRED_TOKENS:
                    return
                token = await grab_token_via_proxy(browser, px)
                if token:
                    await queue.put((token, px))
                    return
                await asyncio.sleep(0.5)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        writer_task = asyncio.create_task(writer())

        tasks = [asyncio.create_task(proxy_worker(px, sem)) for px in proxies]

        async def monitor_target():
            while True:
                await asyncio.sleep(0.2)
                if DESIRED_TOKENS and minted_count >= DESIRED_TOKENS:
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    return
                if all(t.done() for t in tasks):
                    return

        monitor_task = asyncio.create_task(monitor_target())
        _ = await asyncio.gather(*tasks, return_exceptions=True)

        done_event.set()
        await queue.join()
        writer_task.cancel()
        try:
            await writer_task
        except asyncio.CancelledError:
            pass

        await monitor_task
        await browser.close()

    print(f"\nDone. Minted {len(minted_tokens)} tokens → {TOKEN_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
