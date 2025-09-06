# -*- coding: utf-8 -*-
import asyncio
import contextlib
import json
import logging
import os
import random
import re
import shutil
import time
import zipfile
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import partial
from itertools import cycle
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
import tkinter as tk
from pyproj import Transformer
from shapely.geometry import shape as shapely_shape
from shapely.ops import unary_union
from tkinter import filedialog
from tqdm import tqdm

# Playwright (async)
from playwright.async_api import async_playwright

# =========================
# ===== CONFIGURATION =====
# =========================
OUTPUT_DIR      = "Output"
PROXY_FILE      = "PROXYLIST.txt"
DISTRICT_FILE   = "district.json"
LOG_FILE        = "download_tehsil.log"

# ArcGIS layer tokens are IP-bound; we pair each proxy with its own token.
# Strategy for data fetching (after harvesting):
#   "off"    -> no proxy; uses first harvested token (or user token) with direct IP
#   "pin"    -> pin to first harvested proxy+token pair
#   "rotate" -> rotate in lock-step across harvested pairs  (recommended)
PROXY_STRATEGY  = "rotate"   # "off" | "pin" | "rotate"

# Playwright / harvesting settings
HARVEST_TARGET              = 25      # start downloading once this many pairs are ready
MAX_HARVEST_CONCURRENCY     = 6       # how many browsers to launch in parallel
HARVEST_NAV_TIMEOUT_MS      = 30000   # navigation timeout
HARVEST_WAIT_MS             = 2000    # extra settle wait
HARVEST_RETRIES_PER_PROXY   = 2       # attempts per proxy before giving up
HARVEST_HEADLESS            = True    # set False to debug UI

# Token cache & validity
TOKEN_CACHE_FILE    = "token_cache.json"   # per-proxy cache to avoid re-harvest
TOKEN_TTL_MINUTES   = 50                   # consider token fresh for this many minutes

# Download worker settings
MAX_WORKERS         = 2            # threadpool size for batch queries
REQ_TIMEOUT_SEC     = 35           # HTTP timeout for ArcGIS requests
MAX_RETRIES_HTTP    = 3            # retries per batch for HTTP errors

# =========================
# ===== LOGGING SETUP =====
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ],
)

# =========================
# ====== UTILITIES ========
# =========================
TO_WGS84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', '-', str(name).strip())

def ask_output_folder(title="Select destination folder for output") -> Optional[str]:
    root = tk.Tk(); root.withdraw()
    folder = filedialog.askdirectory(title=title, mustexist=True)
    root.destroy()
    return folder or None

def parse_proxy(line: str) -> str:
    parts = line.strip()
    if not parts: return ""
    segs = parts.split(":")
    if len(segs) == 4:
        ip, port, user, pwd = segs
        return f"http://{user}:{pwd}@{ip}:{port}"
    elif len(segs) == 2:
        ip, port = segs
        return f"http://{ip}:{port}"
    raise ValueError(f"Invalid proxy format: {line}")

def load_proxies() -> List[str]:
    if not os.path.exists(PROXY_FILE):
        return []
    with open(PROXY_FILE, "r", encoding="utf-8") as f:
        proxies = [parse_proxy(line) for line in f if line.strip()]
    # shuffle for fair spread
    random.shuffle(proxies)
    return proxies

def _headers() -> Dict[str, str]:
    return {
        "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      f"(KHTML, like Gecko) Chrome/{random.randint(116, 126)}.0.0.0 Safari/537.36"
    }

def fetch_json(url: str, proxy: Optional[str]) -> dict:
    proxies = {"http": proxy, "https": proxy} if proxy else None
    last_err = None
    for attempt in range(1, MAX_RETRIES_HTTP + 1):
        try:
            r = requests.get(url, headers=_headers(), timeout=REQ_TIMEOUT_SEC, proxies=proxies)
            r.raise_for_status()
            resp = r.json()
            if isinstance(resp, dict) and "error" in resp:
                raise RuntimeError(f"ArcGIS error: {resp['error'].get('message')} | details={resp['error'].get('details')}")
            return resp
        except Exception as e:
            last_err = e
            logging.warning(f"Request failed (attempt {attempt}) [{proxy}] {url}: {e}")
            time.sleep(1 + attempt)
    raise RuntimeError(f"Failed after retries on proxy: {proxy} | last error: {last_err}")

def quote_where(where: str) -> str:
    return requests.utils.quote(where, safe="=()' ")

# ==============================
# ===== TOKEN CACHE (per IP) ===
# ==============================
def _load_token_cache() -> dict:
    if not os.path.exists(TOKEN_CACHE_FILE):
        return {}
    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_token_cache(cache: dict) -> None:
    tmp = TOKEN_CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)
    os.replace(tmp, TOKEN_CACHE_FILE)

def _cache_key_for_proxy(proxy_url: str) -> str:
    # Use the full proxy URL as key (includes credentials if any)
    return proxy_url

def _get_cached_token(proxy_url: str) -> Optional[str]:
    cache = _load_token_cache()
    key = _cache_key_for_proxy(proxy_url)
    item = cache.get(key)
    if not item:
        return None
    tok = item.get("token"); ts = item.get("timestamp")
    if not tok or not ts:
        return None
    try:
        ts_dt = datetime.fromisoformat(ts)
    except Exception:
        return None
    if datetime.now(timezone.utc) - ts_dt < timedelta(minutes=TOKEN_TTL_MINUTES):
        logging.info(f"⚡ Using cached token for {proxy_url}")
        return tok
    return None

def _put_cached_token(proxy_url: str, token: str) -> None:
    cache = _load_token_cache()
    key = _cache_key_for_proxy(proxy_url)
    cache[key] = {"token": token, "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    _save_token_cache(cache)

# ==================================
# ===== LAYER & FIELD DISCOVERY =====
# ==================================
def fetch_layer_info(base_url: str, token: str, proxy: Optional[str]) -> dict:
    url = f"{base_url}?f=json&token={token}"
    return fetch_json(url, proxy)

def validate_token_on_layer(base_url: str, token: str, proxy: Optional[str]) -> bool:
    url = f"{base_url}/query?where=1%3D1&returnCountOnly=true&f=json&token={token}"
    _ = fetch_json(url, proxy)
    return True

# Field mapping cache keyed by base_url (avoid unhashable 'proxy' in lru_cache)
FIELD_MAP_CACHE: Dict[str, dict] = {}

def resolve_field_mapping(base_url: str, token: str, proxy: Optional[str]) -> dict:
    """
    Discover real field names and types on this layer (cached by base_url).
    Returns: {"tehsil_id": (fieldName or None, fieldType or None),
              "tehsil_name": (fieldName or None, fieldType or None)}
    """
    if base_url in FIELD_MAP_CACHE:
        return FIELD_MAP_CACHE[base_url]

    info = fetch_layer_info(base_url, token, proxy)
    fields = info.get("fields", []) or []
    lut = {f["name"].lower(): (f["name"], f.get("type")) for f in fields if "name" in f}

    id_candidates   = ["tehsil_id", "tehsilid", "teh_id", "tehcode", "tehsil_code"]
    name_candidates = ["tehsil", "tehsil_name", "tehname", "tehsilname"]

    id_field = next((lut[k] for k in id_candidates if k in lut), (None, None))
    nm_field = next((lut[k] for k in name_candidates if k in lut), (None, None))

    FIELD_MAP_CACHE[base_url] = {"tehsil_id": id_field, "tehsil_name": nm_field}
    return FIELD_MAP_CACHE[base_url]

def build_where_typed(id_field: Optional[str], id_type: Optional[str], tehsil_id_value: str) -> str:
    tid = tehsil_id_value.strip()
    if id_field is None:
        # fallback: impossible WHERE to avoid returning full table by mistake
        return quote_where("1=0")
    if id_type and "String" in id_type:
        return quote_where(f"{id_field}='{tid}'")
    return quote_where(f"{id_field}={tid}")

# ============================
# ===== TEHSIL LIST/COUNT ====
# ============================
def fetch_tehsil_list_dynamic(base_url: str, token: str, proxy: Optional[str],
                              id_field: Optional[str], name_field: Optional[str]) -> List[dict]:
    if not id_field and not name_field:
        return []
    out_fields = []
    if id_field: out_fields.append(id_field)
    if name_field and name_field not in out_fields: out_fields.append(name_field)
    out_param = ",".join(out_fields) if out_fields else "*"
    url = (f"{base_url}/query?where=1%3D1&outFields={out_param}"
           f"&returnDistinctValues=true&returnGeometry=false&f=json&token={token}")
    resp = fetch_json(url, proxy)
    tehsils = []
    for f in resp.get("features", []):
        attrs = f.get("attributes", {})
        tid_val = attrs.get(id_field) if id_field else None
        tname = attrs.get(name_field) if name_field else None
        if tid_val is None and tname is None:
            continue
        tehsils.append({"id": str(tid_val).strip() if tid_val is not None else "",
                        "name": str(tname).strip() if tname is not None else ""})
    unique = {}
    for t in tehsils:
        key = (t["id"], t["name"])
        if key not in unique:
            unique[key] = t
    return sorted(unique.values(), key=lambda x: (x["name"], x["id"]))

def get_total_feature_count(base_url: str, token: str, tehsil_id_str: str,
                            tehsil_name: str, proxy: Optional[str]) -> int:
    validate_token_on_layer(base_url, token, proxy)
    fm = resolve_field_mapping(base_url, token, proxy)
    id_field, id_type = fm["tehsil_id"]
    where_q = build_where_typed(id_field, id_type, tehsil_id_str)
    url = f"{base_url}/query?where={where_q}&returnCountOnly=true&f=json&token={token}"
    resp = fetch_json(url, proxy)
    return resp.get("count", 0)

def fetch_tehsil_batch(base_url: str, token: str, tehsil_id_str: str, tehsil_name: str,
                       offset: int, batch_size: int, proxy: Optional[str]) -> List[dict]:
    fm = resolve_field_mapping(base_url, token, proxy)
    id_field, id_type = fm["tehsil_id"]
    name_field, _name_type = fm["tehsil_name"]
    where_q = build_where_typed(id_field, id_type, tehsil_id_str)

    base_fields = ["Tehsil", "Tehsil_ID", "Mouza", "Mouza_ID", "Type", "M", "A", "K", "SK", "Label", "B", "MN"]
    if id_field and id_field not in base_fields: base_fields.append(id_field)
    if name_field and name_field not in base_fields: base_fields.append(name_field)
    out_fields = ",".join(base_fields)

    url = (f"{base_url}/query?where={where_q}"
           f"&outFields={out_fields}&returnGeometry=true&f=json"
           f"&resultOffset={offset}&resultRecordCount={batch_size}&token={token}")
    resp = fetch_json(url, proxy)
    return resp.get("features", [])

# ============================
# ===== GEOMETRY/OUTPUTS =====
# ============================
def reproject_polygon(rings):
    return [[list(TO_WGS84.transform(x, y)) for x, y in ring] for ring in rings]

def _norm_intish(x):
    if x in [None, "", "None"]:
        return None
    s = str(x)
    return int(s) if s.isdigit() else s

def get_group_key(props: dict) -> str:
    typ = str(props.get("Type", "")).strip()
    m_val = _norm_intish(props.get("M"))
    mn_val = _norm_intish(props.get("MN"))
    b_val = _norm_intish(props.get("B"))
    k_val = _norm_intish(props.get("K"))
    a_val = _norm_intish(props.get("A"))

    if typ in ("KW", "MU/KW"):
        for v in (m_val, a_val, k_val):
            if v not in [None, 0]: return str(v)
        return "0"
    if typ == "MT":
        m_valid = m_val not in [None, 0]
        mn_valid = mn_val not in [None, 0]
        b_valid = b_val not in [None, "", "None", 0, "0"]
        if not m_valid and not mn_valid: return "0"
        if m_valid and mn_valid and b_valid: return str(m_val)
        if not m_valid and mn_valid and b_valid: return f"{b_val}/{mn_val}"
        if m_valid and not mn_valid: return str(m_val)
        if mn_valid and not m_valid: return str(mn_val)
        if m_valid and mn_valid and m_val == mn_val: return str(m_val)
        if mn_valid: return str(mn_val)
        if m_valid: return str(m_val)
        return "0"
    if typ in ("MU", "MU/MT", "MT/MU"):
        if m_val not in [None, 0]: return str(m_val)
    elif typ == "K":
        if k_val not in [None, 0]: return str(k_val)
    if m_val not in [None, 0]: return str(m_val)
    return "0"

def compute_killa(props: dict) -> str:
    def clean(x):
        if x is None: return None
        s = str(x).strip()
        return None if s == "" or s.lower() == "none" else s
    A = clean(props.get("A")); K = clean(props.get("K")); SK = clean(props.get("SK"))
    base = K if (A is not None and K is not None and A == K) else (A if A is not None else K)
    if base is None: return "0"
    return f"{base}_{SK}" if SK is not None else base

def group_and_save(features, district_name, tehsil_name, custom_output_folder=None):
    from shapely.geometry import mapping as shapely_mapping

    per_mouza_all = {}
    per_mouza_groups = {}
    per_mouza_group_polys = {}

    for feat in features:
        props = feat.get("attributes") or feat.get("properties") or {}
        if not props:
            continue
        mouza_name_orig = props.get("Mouza", "Unknown")
        mouza = sanitize_filename(mouza_name_orig)
        geom = feat.get("geometry")
        if not (geom and "rings" in geom):
            continue

        wgs84_coords = reproject_polygon(geom["rings"])
        props = dict(props)
        props["Killa"] = compute_killa(props)
        props["group_key"] = get_group_key(props)

        gj_feat = {
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": wgs84_coords},
            "properties": props
        }

        per_mouza_all.setdefault(mouza, []).append(gj_feat)

        gk = props.get("group_key") or "0"
        if gk != "0":
            per_mouza_groups.setdefault(mouza, {}).setdefault(gk, []).append(gj_feat)
            with contextlib.suppress(Exception):
                poly = shapely_shape(gj_feat["geometry"])
                if not poly.is_valid:
                    poly = poly.buffer(0)
                if poly.is_valid and not poly.is_empty:
                    per_mouza_group_polys.setdefault(mouza, {}).setdefault(gk, []).append(poly)

    tehsil_folder = custom_output_folder if custom_output_folder else os.path.join(
        OUTPUT_DIR, sanitize_filename(district_name), sanitize_filename(tehsil_name)
    )
    os.makedirs(tehsil_folder, exist_ok=True)
    mouza_geojson_dir = os.path.join(tehsil_folder, "Mouza GeoJson")
    os.makedirs(mouza_geojson_dir, exist_ok=True)

    for mouza, feats in per_mouza_all.items():
        undissolved_path = os.path.join(mouza_geojson_dir, f"{mouza}.geojson")
        with open(undissolved_path, "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "features": feats}, f, indent=2)
        print(f"Saved undissolved {undissolved_path}")

        group_polys = per_mouza_group_polys.get(mouza, {})
        if not group_polys:
            print(f"No dissolved geometry for mouza '{mouza}', folder not created.")
            continue

        mouza_dir = os.path.join(tehsil_folder, mouza)
        os.makedirs(mouza_dir, exist_ok=True)

        for gk, gfeats in per_mouza_groups[mouza].items():
            gname_file = sanitize_filename(str(gk))
            g_path = os.path.join(mouza_dir, f"{gname_file}.geojson")
            stripped_feats = []
            for feat in gfeats:
                geom = feat.get("geometry")
                props = feat.get("properties", {})
                killa = props.get("Killa", "0")
                stripped_feats.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {"Killa": killa}
                })
            with open(g_path, "w", encoding="utf-8") as gf:
                json.dump({"type": "FeatureCollection", "features": stripped_feats}, gf, indent=2)

        dissolved_features = []
        for gk, polys in group_polys.items():
            if not polys:
                continue
            with contextlib.suppress(Exception):
                union_geom = unary_union(polys)
                union_geo = shapely_shape(union_geom).mapping if hasattr(unary_union(polys), "mapping") else None
                # The above double-call is heavy; do once properly:
        # Correct dissolve with proper mapping
        dissolved_features = []
        for gk, polys in group_polys.items():
            if not polys:
                continue
            try:
                union_geom = unary_union(polys)
                from shapely.geometry import mapping as shapely_mapping
                union_geo = shapely_mapping(union_geom)
                dissolved_features.append({
                    "type": "Feature",
                    "geometry": union_geo,
                    "properties": {"Murabba_No": gk}
                })
            except Exception as e:
                print(f"Warning: Could not dissolve group '{gk}' for mouza '{mouza}': {e}")

        dissolved_path = os.path.join(tehsil_folder, f"{mouza}.geojson")
        if dissolved_features:
            with open(dissolved_path, "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": dissolved_features}, f, indent=2)
            print(f"Saved dissolved (per-group) {dissolved_path}")
        else:
            print(f"Warning: No valid per-group dissolved geometries for mouza '{mouza}'")

def save_batch(tehsil_folder, offset, features):
    batch_dir = os.path.join(tehsil_folder, "batches"); os.makedirs(batch_dir, exist_ok=True)
    batch_path = os.path.join(batch_dir, f"batch_{offset}.json")
    with open(batch_path, "w", encoding="utf-8") as f:
        json.dump(features, f)
    return batch_path

def read_completed_offsets(tehsil_folder):
    progress_file = os.path.join(tehsil_folder, "download.progress")
    if not os.path.exists(progress_file): return set()
    with open(progress_file, "r", encoding="utf-8") as f:
        return set(int(line.strip()) for line in f if line.strip().isdigit())

def mark_offset_done(tehsil_folder, offset):
    progress_file = os.path.join(tehsil_folder, "download.progress")
    with open(progress_file, "a", encoding="utf-8") as f:
        f.write(f"{offset}\n")

def load_all_batches(tehsil_folder):
    batch_dir = os.path.join(tehsil_folder, "batches")
    all_feats = []
    if os.path.exists(batch_dir):
        for fname in os.listdir(batch_dir):
            if fname.startswith("batch_") and fname.endswith(".json"):
                with open(os.path.join(batch_dir, fname), "r", encoding="utf-8") as f:
                    feats = json.load(f); all_feats.extend(feats)
    return all_feats

# =========================
# PROXY+TOKEN MANAGEMENT
# =========================
class ProxyTokenManager:
    """
    Maintains proxy & token pairing.
    Modes:
      - off:    proxy=None; use first token only
      - pin:    proxy=proxies[0]; use first token only
      - rotate: rotate (proxy, token) together (token_i must match proxy_i)
    """
    def __init__(self, strategy: str, tokens: List[str], proxies: List[str]):
        self.strategy = strategy
        self.tokens = tokens
        self.proxies = proxies
        if self.strategy not in ("off", "pin", "rotate"):
            raise ValueError("PROXY_STRATEGY must be 'off', 'pin', or 'rotate'")

        if self.strategy == "off":
            self._token_iter = cycle([tokens[0]])
            self._proxy_iter = cycle([None])
        elif self.strategy == "pin":
            if not proxies:
                raise ValueError("PROXY_STRATEGY='pin' requires at least one proxy in PROXYLIST.txt")
            self._token_iter = cycle([tokens[0]])
            self._proxy_iter = cycle([proxies[0]])
        else:  # rotate
            if not proxies:
                raise ValueError("PROXY_STRATEGY='rotate' requires proxies")
            n = min(len(tokens), len(proxies))
            if n == 0:
                raise ValueError("No pairs available for rotate mode")
            self.pairs = [(tokens[i], proxies[i]) for i in range(n)]
            self._pair_iter = cycle(self.pairs)

    def next_pair(self) -> Tuple[str, Optional[str]]:
        if self.strategy in ("off", "pin"):
            return next(self._token_iter), next(self._proxy_iter)
        tok, prox = next(self._pair_iter)
        return tok, prox

# =======================================
# TOKEN HARVESTING (with Playwright)
# =======================================
def _proxy_to_playwright(proxy_url: str) -> dict:
    # Convert http://user:pass@host:port -> Playwright proxy dict
    u = urlparse(proxy_url)
    pw = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        pw["username"] = u.username
    if u.password:
        pw["password"] = u.password
    return pw

def _looks_like_arcgis(url: str) -> bool:
    u = url.lower()
    return ("arcgis" in u and ("token=" in u or "f=json" in u))

async def _harvest_token_with_proxy(pw, proxy_url: str) -> Optional[str]:
    """
    Launches a browser with given proxy, opens LIS, and sniffs network requests
    for ArcGIS calls containing 'token=' in querystring.
    """
    browser = None
    context = None
    page = None
    token_found: Optional[str] = None

    try:
        browser = await pw.chromium.launch(headless=HARVEST_HEADLESS)
        context = await browser.new_context(
            proxy=_proxy_to_playwright(proxy_url),
            user_agent=_headers()["User-Agent"],
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        async def route_handler(route, request):
            rt = request.resource_type
            try:
                if rt in ("image", "media", "font", "stylesheet", "websocket", "manifest"):
                    with contextlib.suppress(Exception):
                        await route.abort()
                    return
                with contextlib.suppress(Exception):
                    await route.continue_()
            except Exception:
                # Ignore errors during teardown race
                pass

        await page.route("**/*", route_handler)

        # Listen responses to capture token in querystring
        async def on_response(resp):
            nonlocal token_found
            try:
                url = resp.url
                if token_found is None and _looks_like_arcgis(url):
                    qs = parse_qs(urlparse(url).query)
                    tlist = qs.get("token")
                    if tlist and len(tlist) > 0:
                        token_found = tlist[0]
            except Exception:
                pass

        page.on("response", on_response)

        page.set_default_timeout(HARVEST_NAV_TIMEOUT_MS)
        await page.goto("https://lis.pulse.gop.pk/", wait_until="domcontentloaded")
        await page.wait_for_timeout(HARVEST_WAIT_MS)

        # Also trigger potential map interactions that cause ArcGIS calls
        with contextlib.suppress(Exception):
            await page.evaluate(
                "() => { window.scrollTo(0, document.body.scrollHeight); }"
            )
        await page.wait_for_timeout(1000)

        # Wait a bit more for network activity
        for _ in range(10):
            if token_found:
                break
            await page.wait_for_timeout(500)

        if not token_found:
            raise RuntimeError("Token did not appear in network requests.")

        return token_found

    finally:
        # Ensure routes are removed before closing
        if page:
            with contextlib.suppress(Exception):
                await page.unroute_all(behavior='ignoreErrors')
        if context:
            with contextlib.suppress(Exception):
                await context.close()
        if browser:
            with contextlib.suppress(Exception):
                await browser.close()

async def _precheck_proxy(proxy_url: str) -> bool:
    """
    Quick HEAD to LIS domain using requests via that proxy (synchronously in a thread).
    Avoids burning time on proxies that can't reach LIS.
    """
    def _head():
        try:
            proxies = {"http": proxy_url, "https": proxy_url}
            r = requests.get("https://lis.pulse.gop.pk/", headers=_headers(),
                             proxies=proxies, timeout=10)
            return r.status_code < 500
        except Exception:
            return False
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _head)

async def harvest_tokens_until_threshold(base_url: str, proxies: List[str]) -> Tuple[List[str], List[str]]:
    """
    Returns (tokens, proxies_used) aligned lists.
    Uses cache if fresh; otherwise harvests via Playwright.
    Starts download once threshold reached or when exhausted.
    """
    tokens: List[str] = []
    usable_proxies: List[str] = []

    ready_counter = 0
    target = min(HARVEST_TARGET, len(proxies)) if PROXY_STRATEGY != "off" else 1

    # 1) First, try cached tokens (fast path)
    candidate_pairs = []
    for p in proxies:
        t = _get_cached_token(p)
        if t:
            # verify on base layer; if fails, discard cache
            try:
                validate_token_on_layer(base_url, t, p)
                candidate_pairs.append((t, p))
            except Exception:
                pass

    if candidate_pairs:
        for t, p in candidate_pairs:
            tokens.append(t)
            usable_proxies.append(p)
            ready_counter += 1
            logging.info(f"Pairs ready: {ready_counter}/{target}")
            if ready_counter >= target:
                break

    # 2) If still short, harvest in parallel
    if ready_counter < target and PROXY_STRATEGY != "off":
        remaining = [p for p in proxies if p not in usable_proxies]
        # precheck proxies concurrently to weed out dead ones
        precheck_results = await asyncio.gather(*[_precheck_proxy(p) for p in remaining])
        remaining = [p for p, ok in zip(remaining, precheck_results) if ok]
        dead = [p for p, ok in zip(remaining, precheck_results) if not ok]
        for p in dead:
            logging.error(f"Proxy precheck failed: {p}")

        sem = asyncio.Semaphore(MAX_HARVEST_CONCURRENCY)
        async def harvest_one(pxy: str) -> Optional[Tuple[str, str]]:
            for attempt in range(1, HARVEST_RETRIES_PER_PROXY + 1):
                async with sem:
                    try:
                        async with async_playwright() as pw:
                            tok = await _harvest_token_with_proxy(pw, pxy)
                        # Validate token on layer to ensure it's actually usable
                        try:
                            validate_token_on_layer(base_url, tok, pxy)
                        except Exception as e:
                            raise RuntimeError(f"Fresh token invalid on layer: {e}") from e
                        _put_cached_token(pxy, tok)
                        logging.info(f"✅ Token harvested via proxy {pxy}")
                        return tok, pxy
                    except Exception as e:
                        logging.error(f"Token harvest failed for proxy {pxy} (attempt {attempt}/{HARVEST_RETRIES_PER_PROXY}): {e}")
                        await asyncio.sleep(1.0 + attempt)
            return None

        # Launch tasks in waves until we hit target or run out
        harvest_queue = deque(remaining)
        in_flight: List[asyncio.Task] = []

        while ready_counter < target and (harvest_queue or in_flight):
            # top up
            while len(in_flight) < MAX_HARVEST_CONCURRENCY and harvest_queue:
                pxy = harvest_queue.popleft()
                in_flight.append(asyncio.create_task(harvest_one(pxy)))

            if not in_flight:
                break

            done, pending = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            in_flight = list(pending)
            for d in done:
                res = d.result()
                if res:
                    tok, pxy = res
                    tokens.append(tok)
                    usable_proxies.append(pxy)
                    ready_counter += 1
                    logging.info(f"Pairs ready: {ready_counter}/{target}")
                    if ready_counter >= target:
                        # cancel remaining harvesters gracefully
                        for tsk in pending:
                            tsk.cancel()
                        # Drain cancellations
                        with contextlib.suppress(Exception):
                            await asyncio.gather(*pending, return_exceptions=True)
                        in_flight = []
                        break

    logging.info(f"Using {len(tokens)} proxy-token pairs (threshold={target}).")
    return tokens, usable_proxies

# =========================================
# WORKER: fetch & save a single batch
# =========================================
def fetch_and_save_batch_worker(args):
    tehsil_id_str, tehsil_name, offset, batch_size, base_url, ptm, tehsil_folder = args
    token, proxy = ptm.next_pair()
    try:
        feats = fetch_tehsil_batch(base_url, token, tehsil_id_str, tehsil_name, offset, batch_size, proxy)
        save_batch(tehsil_folder, offset, feats)
        mark_offset_done(tehsil_folder, offset)
        msg = f"[OK] Batch offset {offset} (size {len(feats)}) done for Tehsil_ID={tehsil_id_str}"
        return True, msg, args
    except Exception as e:
        msg = f"[FAILED] Batch offset {offset} for Tehsil_ID={tehsil_id_str}: {e}"
        return False, msg, args

# =========================
# ======== MAIN ===========
# =========================
def main():
    t0 = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Proxy strategy: {PROXY_STRATEGY}")

    answer = input("Is the raw data (GeoJSON) already downloaded? (y/n): ").strip().lower()
    if answer == 'y':
        root = tk.Tk(); root.withdraw()
        file_path = filedialog.askopenfilename(title="Select Raw GeoJSON File",
                                               filetypes=[("GeoJSON Files", "*.geojson"), ("All Files", "*.*")])
        root.destroy()
        if not file_path or not os.path.exists(file_path):
            print("No file selected or file does not exist. Exiting."); return
        print(f"Selected raw file: {file_path}")

        dest_root = ask_output_folder("Select destination folder (root) for saving RAW output")
        if not dest_root:
            print("No destination folder selected. Exiting."); return

        with open(file_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        unique_features = raw_data["features"]

        tehsil_name = None; district_name = None
        if unique_features:
            attr = unique_features[0].get("attributes") or unique_features[0].get("properties")
            if attr:
                tehsil_name = attr.get("Tehsil") or attr.get("tehsil") or attr.get("tehsil_name")
                district_name = attr.get("District") or attr.get("district") or attr.get("district_name")
                if tehsil_name: tehsil_name = sanitize_filename(str(tehsil_name))
                if district_name: district_name = sanitize_filename(str(district_name))
        if not tehsil_name:
            tehsil_name = sanitize_filename(input("Enter Tehsil Name for output folder: ").strip())
        if not district_name:
            district_name = ""

        parent = os.path.join(dest_root, district_name) if district_name else dest_root
        os.makedirs(parent, exist_ok=True)
        tehsil_output_folder = os.path.join(parent, tehsil_name); os.makedirs(tehsil_output_folder, exist_ok=True)

        raw_geojson_path = os.path.join(parent, f"{tehsil_name}-raw.geojson")
        if not os.path.exists(raw_geojson_path):
            with open(raw_geojson_path, "w", encoding="utf-8") as f:
                json.dump(raw_data, f, indent=2)
            print(f"Raw GeoJSON written to: {raw_geojson_path}")
        else:
            print(f"Raw GeoJSON already exists at: {raw_geojson_path}, skipping write.")

        group_and_save(unique_features, district_name, tehsil_name, custom_output_folder=tehsil_output_folder)
        print("Processing complete.")
        logging.info(f"Completed in {time.time()-t0:.1f}s")
        return
    elif answer != 'n':
        print("Invalid input. Please enter 'y' or 'n'."); return

    # Regular workflow
    with open(DISTRICT_FILE, "r", encoding="utf-8") as f:
        districts = json.load(f)

    per_row = 3
    rows = (len(districts) + per_row - 1) // per_row
    print("\nAvailable Districts (ID | Name):")
    print("-" * 70)
    for row in range(rows):
        line = ""
        for col in range(per_row):
            idx = row + col * rows
            if idx < len(districts):
                d = districts[idx]
                entry = f"{d['id']:>2}: {d['name']:<20}"
                line += entry + "   "
        print(line)
    print("-" * 70)

    district_ids = [d["id"] for d in districts]
    while True:
        try:
            sel_id = int(input("\nEnter the District ID from above table: "))
            if sel_id in district_ids:
                sel_district = next(d for d in districts if d["id"] == sel_id)
                BASE_URL = sel_district["url"]
                district_name = sel_district["name"]
                break
            else:
                print("Invalid District ID, try again.")
        except Exception:
            print("Please enter a valid numeric ID.")

    dest_root = ask_output_folder(f"Select destination folder (root) for saving district '{district_name}'")
    if not dest_root:
        print("No destination folder selected. Exiting."); return
    district_folder = os.path.join(dest_root, sanitize_filename(district_name))
    os.makedirs(district_folder, exist_ok=True)

    # Load proxies; harvest tokens until threshold
    proxies = load_proxies()
    if PROXY_STRATEGY == "off":
        # In off-mode, we still need a token. We try cached "DIRECT" key in cache file or harvest using no proxy.
        # For simplicity, we attempt harvest with first proxy if available; else we error.
        if not proxies:
            print("PROXY_STRATEGY='off' requires at least one working path to get a token. Put a proxy in PROXYLIST.txt.")
            return

    # Harvest tokens (or take from cache) until threshold reached
    tokens, usable_proxies = asyncio.run(harvest_tokens_until_threshold(BASE_URL, proxies if PROXY_STRATEGY != "off" else proxies[:1]))

    if not tokens:
        print("No usable tokens could be harvested. Exiting.")
        return

    # Build PTM based on strategy
    if PROXY_STRATEGY == "off":
        ptm = ProxyTokenManager("off", [tokens[0]], [])
    elif PROXY_STRATEGY == "pin":
        ptm = ProxyTokenManager("pin", [tokens[0]], [usable_proxies[0]])
    else:
        ptm = ProxyTokenManager("rotate", tokens, usable_proxies)

    # Introspect layer fields once (validate token too)
    token_tmp, proxy_tmp = ptm.next_pair()
    try:
        validate_token_on_layer(BASE_URL, token_tmp, proxy_tmp)
    except Exception as e:
        print(f"\nToken validation failed on this layer: {e}")
        print("→ Regenerate a fresh token from the SAME IP/proxy path you will use here.\n")
        raise

    fm = resolve_field_mapping(BASE_URL, token_tmp, proxy_tmp)
    id_field, id_type = fm["tehsil_id"]
    name_field, name_type = fm["tehsil_name"]
    print(f"Layer fields detected: Tehsil_ID -> {id_field} ({id_type}) ; Tehsil -> {name_field} ({name_type})")

    # Fetch tehsil list
    tehsils = fetch_tehsil_list_dynamic(BASE_URL, token_tmp, proxy_tmp, id_field, name_field)
    if tehsils:
        print("Fetched tehsils:", tehsils)
        print("\nAvailable Tehsils:")
        for t in tehsils:
            print(f"Tehsil: {t['name'] or '(unknown)'} | ID: {t['id'] or '(blank)'}")
    else:
        print("\nWarning: could not list tehsils from the layer; you can still type IDs manually.")

    tehsil_ids_input = input("\nTehsil ID(s) to process (comma separated): ").strip()
    tehsil_ids = [tid.strip() for tid in tehsil_ids_input.split(",") if tid.strip()]

    name_by_id = {t["id"]: t["name"] for t in tehsils if t["id"]}
    tehsil_map = {}  # name -> [ids]
    for tid in tehsil_ids:
        tname = name_by_id.get(tid, None)
        tehsil_map.setdefault(tname or f"Tehsil_{tid}", []).append(tid)

    # Process each selected tehsil
    for tehsil_name, id_list in tehsil_map.items():
        tehsil_folder = os.path.join(district_folder, sanitize_filename(tehsil_name))
        os.makedirs(tehsil_folder, exist_ok=True)
        tehsil_raw_path = os.path.join(district_folder, f"{sanitize_filename(tehsil_name)}-raw.geojson")

        print(f"\nProcessing tehsil: {tehsil_name}")

        completed_offsets = read_completed_offsets(tehsil_folder)
        all_batches = []
        # Plan batches across all IDs in this tehsil_name bucket
        for tehsil_id_str in id_list:
            token, proxy = ptm.next_pair()
            total_count = get_total_feature_count(BASE_URL, token, tehsil_id_str, tehsil_name, proxy)
            print(f"Total features for Tehsil_ID={tehsil_id_str}: {total_count}")
            batch_sizes = []; offsets = []; current = 0
            while current < total_count:
                batch_size = random.randint(1000, 2000)
                batch_sizes.append(batch_size); offsets.append(current); current += batch_size
            all_batches.extend([(tehsil_id_str, offset, batch_sizes[i]) for i, offset in enumerate(offsets)])

        pending = [(tid, offset, bsize) for (tid, offset, bsize) in all_batches if offset not in completed_offsets]
        print(f"Pending batches for {tehsil_name}: {len(pending)}")

        initial_args = [(tehsil_id_str, tehsil_name, offset, batch_size, BASE_URL, ptm, tehsil_folder)
                        for tehsil_id_str, offset, batch_size in pending]

        failed_batches = []

        print(f"\nStarting batch download for {tehsil_name} ({len(initial_args)} batches)...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_and_save_batch_worker, args) for args in initial_args]
            for f in tqdm(as_completed(futures), total=len(futures), desc=f"Downloading {tehsil_name}", unit="batch"):
                ok, msg, orig = f.result()
                print(msg)
                if not ok:
                    failed_batches.append(orig)

        max_retries = 3
        for retry in range(1, max_retries + 1):
            if not failed_batches:
                break
            print(f"\nRetrying {len(failed_batches)} failed batches for {tehsil_name} (Attempt {retry})...")
            still_failed = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(fetch_and_save_batch_worker, args) for args in failed_batches]
                for f in tqdm(as_completed(futures), total=len(futures), desc=f"Retry {retry} {tehsil_name}", unit="batch"):
                    ok, msg, orig = f.result()
                    print(msg)
                    if not ok:
                        still_failed.append(orig)
            failed_batches = still_failed

        if failed_batches:
            print(f"\nThe following batches FAILED after {max_retries} retries for {tehsil_name}:")
            for args in failed_batches:
                tehsil_id_str, offset, batch_size, *_ = args
                print(f"    Tehsil_ID={tehsil_id_str}, offset={offset}, batch_size={batch_size}")
        else:
            print(f"\nAll batches processed successfully for {tehsil_name} after {max_retries} retries.")

        print("Merging all batches into single GeoJSON for", tehsil_name)
        all_feats = load_all_batches(tehsil_folder)
        unique_features = list({json.dumps(f, sort_keys=True): f for f in all_feats}.values())
        with open(tehsil_raw_path, "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "features": unique_features}, f, indent=2)
        print(f"Raw features saved to {tehsil_raw_path}")

        batches_dir = os.path.join(tehsil_folder, "batches")
        if os.path.exists(batches_dir):
            try:
                shutil.rmtree(batches_dir)
                print(f"Deleted batches folder: {batches_dir}")
            except Exception as e:
                print(f"Warning: Could not delete batches folder: {e}")

        group_and_save(unique_features, district_name, tehsil_name, custom_output_folder=tehsil_folder)

    logging.info(f"Completed in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
