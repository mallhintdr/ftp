# fetch-lis-data-16-8-25.py — Token+Proxy pinned (curl-like POST), Tehsil_ID batching, orderByFields=OBJECTID

import re
import requests
import json
import os
import time
import random
import logging
import shutil
from itertools import cycle
from pyproj import Transformer
from tqdm import tqdm
from shapely.geometry import shape as shapely_shape
from shapely.ops import unary_union
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import tkinter as tk
from tkinter import filedialog
from functools import lru_cache
from typing import Optional, Tuple, List, Dict, Any

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# ===== CONFIGURATION =====
# =========================
OUTPUT_DIR     = "Output"
TOKEN_FILE     = "token.txt"        # each line: <token>,<proxy_url>
DISTRICT_FILE  = "district.json"    # list of dicts: {id, name, url}
LOG_FILE       = "download_tehsil.log"
MAX_WORKERS    = 2                  # batch parallelism (CPU/io bound balance)

# curl-like headers (match your working command)
CURL_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Referer": "https://lis.pulse.gop.pk/",
    "Origin": "https://lis.pulse.gop.pk",
    "Content-Type": "application/x-www-form-urlencoded",
}

# Increase read timeout (your log showed 15s read timeout)
REQ_TIMEOUT = (15, 240)  # (connect, read)
MAX_RETRIES_PER_CLIENT = 3
BACKOFF = 1.6

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

TO_WGS84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

# =========================
# ====== UTILITIES  =======
# =========================
def sanitize_filename(name):
    return re.sub(r'[\\/:*?"<>|]', '-', str(name).strip())

def ask_output_folder(title="Select destination folder for output"):
    root = tk.Tk(); root.withdraw()
    folder = filedialog.askdirectory(title=title, mustexist=True)
    root.destroy()
    return folder or None

# -------- Token+Proxy pinned client ----------
class TokenClient:
    def __init__(self, token: str, proxy_url: str, name: str):
        self.token = token.strip()
        self.proxy = proxy_url.strip()
        self.name  = name
        self.bad_streak = 0
        self.session = requests.Session()
        self.session.verify = False  # -k
        self.session.headers.update(CURL_HEADERS)
        if self.proxy:
            self.session.proxies = {"http": self.proxy, "https": self.proxy}

    def post_json(self, url: str, form: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(form)
        data.setdefault("f", "json")
        data.setdefault("token", self.token)
        r = self.session.post(url, data=data, timeout=REQ_TIMEOUT)
        # bubble up hard HTTP errors
        if r.status_code >= 500 or r.status_code in (403, 404):
            r.raise_for_status()
        try:
            payload = r.json()
        except Exception:
            raise RuntimeError("Non-JSON response from ArcGIS.")
        # ArcGIS returns 200 with error object sometimes
        if isinstance(payload, dict) and "error" in payload:
            msg = payload["error"].get("message", "")
            details = payload["error"].get("details", [])
            raise RuntimeError(f"ArcGIS error: {msg} | details={details}")
        return payload

    def ok(self):
        self.bad_streak = 0

    def bad(self):
        self.bad_streak += 1

# -------- Loader: token.txt lines "token,proxy" ----------
def load_tokens(path: str = TOKEN_FILE) -> List['TokenClient']:
    """
    Load token+proxy pairs from token.txt. Each non-empty line:
        <token>,<proxy_url>
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Token file not found: {path}")
    clients: List[TokenClient] = []
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    idx = 0
    for line in lines:
        if "," in line:
            tok, px = line.split(",", 1)
            tok, px = tok.strip(), px.strip()
        else:
            parts = line.split()
            if len(parts) == 2:
                tok, px = parts[0], parts[1]
            else:
                logging.warning("Skipping malformed token line: %s", line)
                continue
        idx += 1
        clients.append(TokenClient(tok, px, f"T{idx}"))
    if not clients:
        raise ValueError("token.txt has no valid token,proxy lines.")
    return clients

# -------- generic POST+rotate across clients ----------
def post_via_any(url: str, form: Dict[str, Any], clients: List['TokenClient']) -> Dict[str, Any]:
    """
    Try each client (token+proxy) with retries per client until one works.
    """
    if not clients:
        raise RuntimeError("No token+proxy clients available.")
    start = random.randrange(len(clients))
    attempts = 0
    while attempts < len(clients):
        cli = clients[(start + attempts) % len(clients)]
        for a in range(1, MAX_RETRIES_PER_CLIENT + 1):
            try:
                payload = cli.post_json(url, form)
                cli.ok()
                return payload
            except (requests.Timeout, requests.ConnectionError) as e:
                cli.bad()
                logging.warning("[%s] network error (attempt %d/%d): %s", cli.name, a, MAX_RETRIES_PER_CLIENT, e)
                time.sleep(BACKOFF * a)
            except RuntimeError as e:
                # ArcGIS error or non-JSON
                cli.bad()
                logging.warning("[%s] %s", cli.name, e)
                # retry a couple times on the same client in case it's transient
                time.sleep(BACKOFF * a)
        attempts += 1
    raise RuntimeError("All token/proxy clients failed for this request.")

# =========================
# ==== LAYER / TOKENS  ====
# =========================
def fetch_json(url: str, *, timeout: int = 35) -> dict:
    """
    (kept for completeness; not used for ArcGIS after proxy upgrade)
    Basic GET with retries that returns parsed JSON (or raises).
    """
    headers = {
        "User-Agent": (
            f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{random.randint(115,126)}.0.0.0 Safari/537.36"
        )
    }
    last_err = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=timeout, verify=False)
            r.raise_for_status()
            resp = r.json()
            if isinstance(resp, dict) and "error" in resp:
                raise RuntimeError(
                    f"ArcGIS error: {resp['error'].get('message')} | "
                    f"details={resp['error'].get('details')}"
                )
            return resp
        except Exception as e:
            last_err = e
            logging.warning(f"Request failed (attempt {attempt+1}) {url}: {e}")
            time.sleep(2 + attempt)
    raise RuntimeError(f"Failed after retries. Last error: {last_err}")

def fetch_layer_info_post(base_url: str, clients: List['TokenClient']) -> dict:
    url = f"{base_url}?f=json"
    return post_via_any(url, {}, clients)

def validate_token_on_layer_post(base_url: str, clients: List['TokenClient']) -> bool:
    url = f"{base_url}/query"
    _ = post_via_any(url, {"where": "1=1", "returnCountOnly": "true"}, clients)
    return True

# Cache layer fields per base_url
_LAYER_FIELDS_CACHE: Dict[str, Dict[str, Tuple[Optional[str], Optional[str]]]] = {}
_LAYER_MAXREC_CACHE: Dict[str, int] = {}
# Track available field names to avoid requesting invalid ones
_LAYER_AVAILABLE_FIELDS: Dict[str, Set[str]] = {}

def resolve_field_mapping(base_url: str, clients: List['TokenClient']) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """
    Discover real field names and types on this layer.
    Returns: {
      'tehsil_id':   (fieldName or None, fieldType or None),
      'tehsil_name': (fieldName or None, fieldType or None)
    }
    """
    if base_url in _LAYER_FIELDS_CACHE:
        return _LAYER_FIELDS_CACHE[base_url]
    info = fetch_layer_info_post(base_url, clients)

    fields = info.get("fields", []) or []
    lut = {f["name"].lower(): (f["name"], f.get("type")) for f in fields if "name" in f}
    # Remember the set of available field names (case-sensitive)
    _LAYER_AVAILABLE_FIELDS[base_url] = {f["name"] for f in fields if "name" in f}

    id_candidates   = ["tehsil_id", "tehsilid", "teh_id", "tehcode", "tehsil_code"]
    name_candidates = ["tehsil", "tehsil_name", "tehname", "tehsilname"]

    id_field   = next((lut[k] for k in id_candidates   if k in lut), (None, None))
    name_field = next((lut[k] for k in name_candidates if k in lut), (None, None))

    # capture maxRecordCount for saner batch sizing
    mrc = info.get("maxRecordCount")
    if isinstance(mrc, int) and mrc > 0:
        _LAYER_MAXREC_CACHE[base_url] = mrc

    _LAYER_FIELDS_CACHE[base_url] = {"tehsil_id": id_field, "tehsil_name": name_field}
    return _LAYER_FIELDS_CACHE[base_url]

def get_max_record_count(base_url: str, clients: List['TokenClient'], default_low: int = 1500) -> int:
    if base_url not in _LAYER_MAXREC_CACHE:
        # populate via layer info call
        resolve_field_mapping(base_url, clients)
    return max(1, _LAYER_MAXREC_CACHE.get(base_url, default_low))

def quote_where(where: str) -> str:
    """
    For POST forms we pass raw SQL where (requests will encode).
    """
    return where

def build_where_typed(id_field: str, id_type: Optional[str], tehsil_id_value: str) -> str:
    tid = tehsil_id_value.strip()
    if not id_field:
        return "1=1"
    if id_type and "String" in id_type:
        return quote_where(f"{id_field}='{tid}'")
    else:
        return quote_where(f"{id_field}={tid}")

# =========================
# ===== TEHSIL LISTING ====
# =========================
def fetch_tehsil_list_dynamic(base_url: str, clients: List['TokenClient'],
                              id_field: Optional[str],
                              name_field: Optional[str]) -> List[Dict[str, str]]:
    if not id_field and not name_field:
        return []
    out_fields = []
    if id_field: out_fields.append(id_field)
    if name_field and name_field not in out_fields: out_fields.append(name_field)
    out_fields_param = ",".join(out_fields) if out_fields else "*"

    url = f"{base_url}/query"
    form = {
        "where": "1=1",
        "outFields": out_fields_param,
        "returnDistinctValues": "true",
        "returnGeometry": "false",
    }
    resp = post_via_any(url, form, clients)
    tehsils = []
    for f in resp.get("features", []):
        attrs = f.get("attributes", {})
        tid_val = attrs.get(id_field) if id_field else None
        tname   = attrs.get(name_field) if name_field else None
        if tid_val is None and tname is None:
            continue
        tehsils.append({
            "id": str(tid_val).strip() if tid_val is not None else "",
            "name": str(tname).strip() if tname is not None else ""
        })
    # Deduplicate & sort
    unique = {}
    for t in tehsils:
        key = (t["id"], t["name"])
        if key not in unique:
            unique[key] = t
    return sorted(unique.values(), key=lambda x: (x["name"], x["id"]))

# =========================
# ====== COUNT / BATCH ====
# =========================
def get_total_feature_count(base_url: str, clients: List['TokenClient'],
                            tehsil_id_str: str, tehsil_name: str) -> int:
    # ensure layer accessible with at least one client
    validate_token_on_layer_post(base_url, clients)
    fm = resolve_field_mapping(base_url, clients)
    id_field, id_type = fm["tehsil_id"]
    where_q = build_where_typed(id_field, id_type, tehsil_id_str)
    url = f"{base_url}/query"
    form = {"where": where_q, "returnCountOnly": "true"}
    resp = post_via_any(url, form, clients)
    return int(resp.get("count", 0))

def fetch_tehsil_batch(base_url: str, clients: List['TokenClient'],
                       tehsil_id_str: str, tehsil_name: str,
                       offset: int, batch_size: int) -> List[dict]:
    fm = resolve_field_mapping(base_url, clients)
    id_field, id_type = fm["tehsil_id"]
    name_field, _ = fm["tehsil_name"]
    where_q = build_where_typed(id_field, id_type, tehsil_id_str)

    base_fields = [
        "Tehsil", "Tehsil_ID", "Mouza", "Mouza_ID",
        "Type", "M", "A", "K", "SK", "Label", "B", "MN"
    ]
    # Keep only fields actually present on the layer
    available = _LAYER_AVAILABLE_FIELDS.get(base_url, set())
    base_fields = [f for f in base_fields if f in available]
    if id_field and id_field not in base_fields: base_fields.append(id_field)
    if name_field and name_field not in base_fields: base_fields.append(name_field)
    out_fields = ",".join(base_fields)


url = f"{base_url}/query"
    form = {
        "where": where_q,
        "outFields": out_fields,
        "returnGeometry": "true",
        # *** CRITICAL: needed for ArcGIS pagination ***
        "orderByFields": "OBJECTID",
        "resultOffset": str(offset),
        "resultRecordCount": str(batch_size),
        # leave outSR unspecified; we reproject from 3857->4326 downstream
        # "sqlFormat": "standard"  # optional; usually not required
    }
    resp = post_via_any(url, form, clients)
    return resp.get("features", []) or []

# =========================
# ==== GEOMETRY / I/O  ====
# =========================
def reproject_polygon(rings):
    return [[list(TO_WGS84.transform(x, y)) for x, y in ring] for ring in rings]

def get_group_key(props):
    typ = str(props.get("Type", "")).strip()
    m_val = props.get("M"); mn_val = props.get("MN"); b_val = props.get("B"); k_val = props.get("K"); a_val = props.get("A")
    def norm(x): return None if x in [None, "", "None"] else (int(x) if str(x).isdigit() else x)
    m_norm = norm(m_val); mn_norm = norm(mn_val); b_norm = norm(b_val); k_norm = norm(k_val); a_norm = norm(a_val)
    if typ in ("KW", "MU/KW"):
        for v in (m_norm, a_norm, k_norm):
            if v not in [None, 0]: return str(v)
        return "0"
    if typ == "MT":
        m_valid = m_norm not in [None, 0]; mn_valid = mn_norm not in [None, 0]; b_valid = b_norm not in [None, "", "None", 0, "0"]
        if not m_valid and not mn_valid: return "0"
        if m_valid and mn_valid and b_valid: return str(m_norm)
        if not m_valid and mn_valid and b_valid: return f"{b_norm}/{mn_norm}"
        if m_valid and not mn_valid: return str(m_norm)
        if mn_valid and not m_valid: return str(mn_norm)
        if m_valid and mn_valid and m_norm == mn_norm: return str(m_norm)
        if mn_valid: return str(mn_norm)
        if m_valid: return str(m_norm)
        return "0"
    if typ in ("MU", "MU/MT", "MT/MU"):
        if m_norm not in [None, 0]: return str(m_norm)
    elif typ == "K":
        if k_norm not in [None, 0]: return str(k_norm)
    if m_norm not in [None, 0]: return str(m_norm)
    return "0"

def compute_killa(props):
    def clean(x):
        if x is None: return None
        s = str(x).strip(); return None if s == "" or s.lower() == "none" else s
    A = clean(props.get("A")); K = clean(props.get("K")); SK = clean(props.get("SK"))
    if A is not None: base = K if (K is not None and A == K) else A
    else: base = K
    if base is None: return "0"
    return f"{base}_{SK}" if SK is not None else base

def group_and_save(features, district_name, tehsil_name, custom_output_folder=None):
    from shapely.ops import unary_union
    from shapely.geometry import mapping as shapely_mapping

    per_mouza_all = {}
    per_mouza_groups = {}
    per_mouza_group_polys = {}

    for feat in features:
        props = feat.get("attributes") or feat.get("properties")
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
            try:
                poly = shapely_shape(gj_feat["geometry"])
                if not poly.is_valid:
                    poly = poly.buffer(0)
                if poly.is_valid and not poly.is_empty:
                    per_mouza_group_polys.setdefault(mouza, {}).setdefault(gk, []).append(poly)
            except Exception as e:
                print(f"[Warning] Invalid geometry in mouza '{mouza}', group '{gk}': {e}")

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
            try:
                union_geom = unary_union(polys)
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

def save_batch(tehsil_folder: str, offset: int, features: List[dict]) -> str:
    batch_dir = os.path.join(tehsil_folder, "batches"); os.makedirs(batch_dir, exist_ok=True)
    batch_path = os.path.join(batch_dir, f"batch_{offset}.json")
    with open(batch_path, "w", encoding="utf-8") as f:
        json.dump(features, f)
    return batch_path

def read_completed_offsets(tehsil_folder: str) -> set:
    progress_file = os.path.join(tehsil_folder, "download.progress")
    if not os.path.exists(progress_file): return set()
    with open(progress_file, "r", encoding="utf-8") as f:
        return set(int(line.strip()) for line in f if line.strip().isdigit())

def mark_offset_done(tehsil_folder: str, offset: int) -> None:
    progress_file = os.path.join(tehsil_folder, "download.progress")
    with open(progress_file, "a", encoding="utf-8") as f:
        f.write(f"{offset}\n")

def load_all_batches(tehsil_folder: str) -> List[dict]:
    batch_dir = os.path.join(tehsil_folder, "batches")
    all_feats = []
    if os.path.exists(batch_dir):
        for fname in os.listdir(batch_dir):
            if fname.startswith("batch_") and fname.endswith(".json"):
                with open(os.path.join(batch_dir, fname), "r", encoding="utf-8") as f:
                    feats = json.load(f); all_feats.extend(feats)
    return all_feats

# =========================
# ======== WORKERS ========
# =========================
def token_for_batch(tokens: List['TokenClient'], offset: int, idx_hint: int = 0) -> 'TokenClient':
    """
    Deterministic client selection per batch to avoid thread contention:
    uses offset plus optional hint to pick a client index.
    """
    if not tokens:
        raise ValueError("No token+proxy clients available.")
    return tokens[(offset + idx_hint) % len(tokens)]

def fetch_and_save_batch_worker(args):
    tehsil_id_str, tehsil_name, offset, batch_size, base_url, tokens, tehsil_folder, idx_hint = args
    try:
        feats = fetch_tehsil_batch(base_url, tokens, tehsil_id_str, tehsil_name, offset, batch_size)
        save_batch(tehsil_folder, offset, feats)
        mark_offset_done(tehsil_folder, offset)
        msg = f"[OK] Batch offset {offset} (size {len(feats)}) done for Tehsil_ID={tehsil_id_str}"
        return True, msg, args
    except Exception as e:
        msg = f"[FAILED] Batch offset {offset} for Tehsil_ID={tehsil_id_str}: {e}"
        return False, msg, args

# =========================
# ========= MAIN ==========
# =========================
if __name__ == "__main__":
    t0 = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Proxy strategy: token-pinned (uses the proxy that minted the token)")

    # Raw mode?
    answer = input("Is the raw data (GeoJSON) already downloaded? (y/n): ").strip().lower()
    if answer == 'y':
        root = tk.Tk(); root.withdraw()
        file_path = filedialog.askopenfilename(title="Select Raw GeoJSON File",
                                               filetypes=[("GeoJSON Files", "*.geojson"), ("All Files", "*.*")])
        root.destroy()
        if not file_path or not os.path.exists(file_path):
            print("No file selected or file does not exist. Exiting."); exit(1)
        print(f"Selected raw file: {file_path}")

        dest_root = ask_output_folder("Select destination folder (root) for saving RAW output")
        if not dest_root:
            print("No destination folder selected. Exiting."); exit(1)

        with open(file_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        unique_features = raw_data.get("features", [])

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
        logging.info(f"Completed in {time.time() - t0:.1f}s")
        exit(0)

    elif answer != 'n':
        print("Invalid input. Please enter 'y' or 'n'."); exit(1)

    # Regular workflow: load districts and token+proxy clients
    if not os.path.exists(DISTRICT_FILE):
        raise FileNotFoundError(f"District file not found: {DISTRICT_FILE}")
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
        print("No destination folder selected. Exiting."); exit(1)
    district_folder = os.path.join(dest_root, sanitize_filename(district_name))
    os.makedirs(district_folder, exist_ok=True)

    tokens = load_tokens()  # now returns List[TokenClient]
    print(f"{len(tokens)} token+proxy pair(s) loaded.")

    # Validate & detect fields (cycle through clients until one works)
    tehsils = []
    tried = 0
    last_err = None
    while not tehsils and tried < len(tokens):
        try:
            # try layer access & field mapping through all clients (post_via_any rotates)
            validate_token_on_layer_post(BASE_URL, tokens)
            fm = resolve_field_mapping(BASE_URL, tokens)
            id_field, id_type = fm["tehsil_id"]
            name_field, name_type = fm["tehsil_name"]
            print(f"Layer fields detected: Tehsil_ID -> {id_field} ({id_type}) ; Tehsil -> {name_field} ({name_type})")
            tehsils = fetch_tehsil_list_dynamic(BASE_URL, tokens, id_field, name_field)
            if tehsils:
                break
        except Exception as e:
            last_err = e
            print(f"[!] Listing attempt failed; rotating tokens… ({e})")
        finally:
            tried += 1

    if not tehsils and last_err:
        print("\nWarning: could not list tehsils from the layer; you can still type IDs manually.")
    else:
        print("Fetched tehsils:", tehsils)
        print("\nAvailable Tehsils:")
        for t in tehsils:
            print(f"Tehsil: {t['name'] or '(unknown)'} | ID: {t['id'] or '(blank)'}")

    tehsil_ids_input = input("\nTehsil ID(s) to process (comma separated): ").strip()
    tehsil_ids = [tid.strip() for tid in tehsil_ids_input.split(",") if tid.strip()]

    name_by_id = {t["id"]: t["name"] for t in tehsils if t["id"]}
    tehsil_map = {}  # name -> [ids]
    for tid in tehsil_ids:
        tname = name_by_id.get(tid, None)
        tehsil_map.setdefault(tname or f"Tehsil_{tid}", []).append(tid)

    # For smarter batch sizing, respect layer maxRecordCount if present
    layer_max = get_max_record_count(BASE_URL, tokens)
    # keep your previous randomness but cap at server max
    BATCH_MIN = min(1000, layer_max)
    BATCH_MAX = min(2000, layer_max)
    if BATCH_MIN > BATCH_MAX:
        BATCH_MIN = BATCH_MAX

    # Process each logical tehsil group
    for tehsil_name, id_list in tehsil_map.items():
        tehsil_folder = os.path.join(district_folder, sanitize_filename(tehsil_name))
        os.makedirs(tehsil_folder, exist_ok=True)
        tehsil_raw_path = os.path.join(district_folder, f"{sanitize_filename(tehsil_name)}-raw.geojson")

        print(f"\nProcessing tehsil: {tehsil_name}")

        completed_offsets = read_completed_offsets(tehsil_folder)
        all_batches = []

        # Build all batches for all IDs first
        for tehsil_id_str in id_list:
            total_count = get_total_feature_count(BASE_URL, tokens, tehsil_id_str, tehsil_name)
            print(f"Total features for Tehsil_ID={tehsil_id_str}: {total_count}")

            batch_sizes = []
            offsets = []
            current = 0
            while current < total_count:
                batch_size = random.randint(BATCH_MIN, BATCH_MAX) if BATCH_MIN > 0 else 1500
                batch_sizes.append(batch_size)
                offsets.append(current)
                current += batch_size
            all_batches.extend([(tehsil_id_str, offset, batch_sizes[i]) for i, offset in enumerate(offsets)])

        # Filter out completed offsets (resume support)
        pending = [(tid, offset, bsize) for (tid, offset, bsize) in all_batches if offset not in completed_offsets]
        print(f"Pending batches for {tehsil_name}: {len(pending)}")

        # Prepare worker args
        initial_args = [
            (tehsil_id_str, tehsil_name, offset, batch_size, BASE_URL, tokens, tehsil_folder, i)
            for i, (tehsil_id_str, offset, batch_size) in enumerate(pending)
        ]

        failed_batches = []
        print(f"\nStarting batch download for {tehsil_name} ({len(initial_args)} batches)...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_and_save_batch_worker, args) for args in initial_args]
            for f in tqdm(as_completed(futures), total=len(futures), desc=f"Downloading {tehsil_name}", unit="batch"):
                ok, msg, orig = f.result()
                print(msg)
                if not ok:
                    failed_batches.append(orig)

        # Retry loop
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

        # Merge & write raw collection
        print("Merging all batches into single GeoJSON for", tehsil_name)
        all_feats = load_all_batches(tehsil_folder)
        unique_features = list({json.dumps(f, sort_keys=True): f for f in all_feats}.values())
        with open(tehsil_raw_path, "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "features": unique_features}, f, indent=2)
        print(f"Raw features saved to {tehsil_raw_path}")

        # Cleanup batches
        batches_dir = os.path.join(tehsil_folder, "batches")
        if os.path.exists(batches_dir):
            try:
                shutil.rmtree(batches_dir)
                print(f"Deleted batches folder: {batches_dir}")
            except Exception as e:
                print(f"Warning: Could not delete batches folder: {e}")

        # Group & dissolve outputs
        group_and_save(unique_features, district_name, tehsil_name, custom_output_folder=tehsil_folder)

    logging.info(f"Completed in {time.time()-t0:.1f}s")
