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
from typing import Optional, Tuple

# =========================
# ===== CONFIGURATION =====
# =========================
OUTPUT_DIR     = "Output"
TOKEN_FILE     = "token.txt"
PROXY_FILE     = "PROXYLIST.txt"
DISTRICT_FILE  = "district.json"
LOG_FILE       = "download_tehsil.log"
MAX_WORKERS    = 2

# Proxy/Token strategy (tokens are IP-bound on ArcGIS)
#   "off"    -> no proxy; use tokens issued from your machine IP
#   "pin"    -> use first proxy from PROXYLIST.txt for ALL requests (issue tokens via that proxy)
#   "rotate" -> rotate proxies; ONLY if token_i was issued from proxy_i (same order/length as proxies)
PROXY_STRATEGY = "off"   # "off" | "pin" | "rotate"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

TO_WGS84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

def sanitize_filename(name):
    return re.sub(r'[\\/:*?"<>|]', '-', str(name).strip())

def ask_output_folder(title="Select destination folder for output"):
    root = tk.Tk(); root.withdraw()
    folder = filedialog.askdirectory(title=title, mustexist=True)
    root.destroy()
    return folder or None

def parse_proxy(line):
    parts = line.strip().split(':')
    if len(parts) == 4:
        ip, port, user, pwd = parts
        return f"http://{user}:{pwd}@{ip}:{port}"
    elif len(parts) == 2:
        ip, port = parts
        return f"http://{ip}:{port}"
    raise ValueError(f"Invalid proxy format: {line}")

def load_proxies():
    if not os.path.exists(PROXY_FILE):
        return []
    with open(PROXY_FILE, "r", encoding="utf-8") as f:
        proxies = [parse_proxy(line) for line in f if line.strip()]
    random.shuffle(proxies)  # <-- SHUFFLE PROXIES BEFORE RETURNING
    return proxies


def load_tokens():
    with open(TOKEN_FILE, "r", encoding="utf-8") as f:
        tokens = [line.strip() for line in f if line.strip()]
    if not tokens:
        raise Exception("No tokens found in token.txt")
    return tokens

def fetch_json(url, proxy):
    headers = {
        "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(115, 126)}.0.0.0 Safari/537.36"
    }
    proxies = {"http": proxy, "https": proxy} if proxy else None
    last_err = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=35, proxies=proxies)
            r.raise_for_status()
            resp = r.json()
            if isinstance(resp, dict) and "error" in resp:
                raise RuntimeError(f"ArcGIS error: {resp['error'].get('message')} | details={resp['error'].get('details')}")
            return resp
        except Exception as e:
            last_err = e
            logging.warning(f"Request failed (attempt {attempt+1}) [{proxy}] {url}: {e}")
            time.sleep(2 + attempt)
    raise RuntimeError(f"Failed after retries on proxy: {proxy} | last error: {last_err}")

# -------- Layer & token helpers --------
def fetch_layer_info(base_url, token, proxy):
    url = f"{base_url}?f=json&token={token}"
    return fetch_json(url, proxy)

def validate_token_on_layer(base_url, token, proxy):
    url = f"{base_url}/query?where=1%3D1&returnCountOnly=true&f=json&token={token}"
    _ = fetch_json(url, proxy)
    return True

@lru_cache(maxsize=128)
def resolve_field_mapping(base_url, token, proxy):
    """
    Discover real field names and types on this layer.
    Returns: {
      'tehsil_id':   (fieldName or None, fieldType or None),
      'tehsil_name': (fieldName or None, fieldType or None)
    }
    """
    info = fetch_layer_info(base_url, token, proxy)
    fields = info.get("fields", []) or []
    lut = {f["name"].lower(): (f["name"], f.get("type")) for f in fields if "name" in f}

    id_candidates   = ["tehsil_id", "tehsilid", "teh_id", "tehcode", "tehsil_code"]
    name_candidates = ["tehsil", "tehsil_name", "tehname", "tehsilname"]

    id_field = next((lut[k] for k in id_candidates if k in lut), (None, None))
    nm_field = next((lut[k] for k in name_candidates if k in lut), (None, None))
    return {"tehsil_id": id_field, "tehsil_name": nm_field}

def quote_where(where: str) -> str:
    # safe chars: = ( ) ' space
    return requests.utils.quote(where, safe="=()' ")

def build_where_typed(id_field, id_type, tehsil_id_value):
    tid = tehsil_id_value.strip()
    if id_type and "String" in id_type:
        return quote_where(f"{id_field}='{tid}'")
    else:
        return quote_where(f"{id_field}={tid}")

# ---------- Tehsil listing (dynamic fields) ----------
def fetch_tehsil_list_dynamic(base_url, token, proxy, id_field: Optional[str], name_field: Optional[str]):
    # If fields missing, return empty (user can still type IDs manually)
    if not id_field and not name_field:
        return []

    out_fields = []
    if id_field: out_fields.append(id_field)
    if name_field and name_field not in out_fields: out_fields.append(name_field)
    out_fields_param = ",".join(out_fields) if out_fields else "*"

    url = (f"{base_url}/query?where=1%3D1"
           f"&outFields={out_fields_param}"
           f"&returnDistinctValues=true&returnGeometry=false&f=json&token={token}")
    resp = fetch_json(url, proxy)
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
    # Deduplicate
    unique = {}
    for t in tehsils:
        key = (t["id"], t["name"])
        if key not in unique:
            unique[key] = t
    # Sort by name then id for nicer display
    return sorted(unique.values(), key=lambda x: (x["name"], x["id"]))

# ---------- Count & batch using typed WHERE ----------
def get_total_feature_count(base_url, token, tehsil_id_str, tehsil_name, proxy):
    validate_token_on_layer(base_url, token, proxy)
    fm = resolve_field_mapping(base_url, token, proxy)
    id_field, id_type = fm["tehsil_id"]
    name_field, name_type = fm["tehsil_name"]
    where_q = build_where_typed(id_field, id_type, tehsil_id_str)
    url = f"{base_url}/query?where={where_q}&returnCountOnly=true&f=json&token={token}"
    resp = fetch_json(url, proxy)
    return resp.get("count", 0)

def fetch_tehsil_batch(base_url, token, tehsil_id_str, tehsil_name, offset, batch_size, proxy):
    fm = resolve_field_mapping(base_url, token, proxy)
    id_field, id_type = fm["tehsil_id"]
    name_field, name_type = fm["tehsil_name"]
    where_q = build_where_typed(id_field, id_type, tehsil_id_str)

    # conservative field list; add resolved names if missing
    base_fields = ["Tehsil", "Tehsil_ID", "Mouza", "Mouza_ID", "Type", "M", "A", "K", "SK", "Label", "B", "MN"]
    if id_field and id_field not in base_fields: base_fields.append(id_field)
    if name_field and name_field not in base_fields: base_fields.append(name_field)
    out_fields = ",".join(base_fields)

    url = (f"{base_url}/query?where={where_q}"
           f"&outFields={out_fields}&returnGeometry=true&f=json"
           f"&resultOffset={offset}&resultRecordCount={batch_size}&token={token}")
    resp = fetch_json(url, proxy)
    return resp.get("features", [])

# ---------- Geometry & outputs ----------
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
        props = feat.get("attributes")
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
        # Always save undissolved GeoJSON
        undissolved_path = os.path.join(mouza_geojson_dir, f"{mouza}.geojson")
        with open(undissolved_path, "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "features": feats}, f, indent=2)
        print(f"Saved undissolved {undissolved_path}")

        # If no dissolved groups, skip creating folder
        group_polys = per_mouza_group_polys.get(mouza, {})
        if not group_polys:
            print(f"No dissolved geometry for mouza '{mouza}', folder not created.")
            continue

        # Create folder for dissolved per-group killa files
        mouza_dir = os.path.join(tehsil_folder, mouza)
        os.makedirs(mouza_dir, exist_ok=True)

        # Save per-group killa-only files
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

        # Save dissolved per-group geometries (Murabba_No)
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

# ================
# PROXY MANAGEMENT
# ================
class ProxyTokenManager:
    """
    Ensures proxy & token pairing consistent with ArcGIS IP-bound tokens.
    Modes:
      - off:    proxy=None; use first token only
      - pin:    proxy=proxies[0]; use first token only (issue token via that proxy)
      - rotate: rotate (proxy, token) together in lockstep (token_i must match proxy_i)
    """
    def __init__(self, strategy, tokens, proxies):
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
            if len(tokens) != len(proxies):
                logging.warning("In 'rotate' mode, supply SAME number of tokens and proxies (token_i must match proxy_i). Using min length.")
            n = min(len(tokens), len(proxies))
            if n == 0:
                raise ValueError("No pairs available for rotate mode")
            self.pairs = [(tokens[i], proxies[i]) for i in range(n)]
            self._pair_iter = cycle(self.pairs)

    def next_pair(self):
        if self.strategy in ("off", "pin"):
            return next(self._token_iter), next(self._proxy_iter)
        else:
            tok, prox = next(self._pair_iter)
            return tok, prox

# Worker used by ThreadPool
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

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    t0 = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Proxy strategy: {PROXY_STRATEGY}")

    # Step 1: Raw mode?
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
        logging.info(f"Completed in {time.time() - t0:.1f}s")
        exit(0)

    elif answer != 'n':
        print("Invalid input. Please enter 'y' or 'n'."); exit(1)

    # Step 2: Regular workflow
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

    tokens  = load_tokens()
    proxies = load_proxies()
    ptm = ProxyTokenManager(PROXY_STRATEGY, tokens, proxies)

    # Introspect layer fields once (also validates token)
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

    # Fetch tehsil list using resolved fields
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

    # Build name->ids from listing (if available)
    name_by_id = {t["id"]: t["name"] for t in tehsils if t["id"]}
    tehsil_map = {}  # name -> [ids]
    for tid in tehsil_ids:
        tname = name_by_id.get(tid, None)
        if tname is None and not tehsils:
            # If we couldn't list, ask once for a name fallback (optional)
            tname = None
        tehsil_map.setdefault(tname or f"Tehsil_{tid}", []).append(tid)

    # Process
    for tehsil_name, id_list in tehsil_map.items():
        tehsil_folder = os.path.join(district_folder, sanitize_filename(tehsil_name))
        os.makedirs(tehsil_folder, exist_ok=True)
        tehsil_raw_path = os.path.join(district_folder, f"{sanitize_filename(tehsil_name)}-raw.geojson")

        print(f"\nProcessing tehsil: {tehsil_name}")

        completed_offsets = read_completed_offsets(tehsil_folder)
        all_batches = []
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

        max_retries = 3
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
