"""
AMT Procurement - Single-file Flask Backend (Excel DB)
FIXED: Smart Auto-Backup (Works on Sleeping/Free Servers)
"""

import os
import json
import hashlib
import shutil
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
import openpyxl
from openpyxl import Workbook
from flask import Flask, jsonify, request, session, send_from_directory, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename

# -------------------------------------------------
# App init
# -------------------------------------------------
app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = os.environ.get("SECRET_KEY", "AMT_SECRET_KEY_CHANGE_ME")

CORS(app, supports_credentials=True, origins=[
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "https://amt-procurement.onrender.com",
])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "office_ops.xlsx")

DEFAULT_ADMIN = {"username": "admin", "password": "admin123", "role": "admin"}
FX_CACHE_FILE = os.path.join(BASE_DIR, "fx_cache.json")
FX_CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 hours

BACKUP_DIR = os.path.join(BASE_DIR, "backups")
ALLOWED_UPLOAD_EXT = {".xlsx"}
AUTO_BACKUP_INTERVAL_SECONDS = 12 * 60 * 60 # 12 Hours

SHEETS: Dict[str, List[str]] = {
    "users": ["username", "password_hash", "role", "created_at"],
    "requisitions": [
        "id", "number", "description", "vessel", "category", "supplier",
        "date_ordered", "expected",
        "amount_original", "currency", "amount_usd",
        "paid", "delivered", "status",
        "po_number", "remarks", "urgency",
        "created_by", "created_at", "updated_at"
    ],
    "landings": [
        "id", "vessel", "item", "workshop",
        "expected", "landed_date",
        "amount_original", "currency", "amount_usd",
        "paid", "delivered", "status",
        "created_by", "created_at", "updated_at"
    ],
    "directory": [
        "id", "type", "name", "email", "phone", "address",
        "created_by", "created_at"
    ],
    "categories": ["id", "name", "abbr", "created_at"],
    "vessels": ["id", "name", "created_at"],
    "logs": ["timestamp", "action", "details"],
}

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

def ensure_db() -> None:
    if not os.path.exists(DB_FILE):
        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            wb.remove(wb["Sheet"])
        for sname, headers in SHEETS.items():
            ws = wb.create_sheet(sname)
            ws.append(headers)
        wb.save(DB_FILE)

    wb = openpyxl.load_workbook(DB_FILE)

    # Header migration
    for sname, headers in SHEETS.items():
        if sname not in wb.sheetnames:
            ws_new = wb.create_sheet(sname)
            ws_new.append(headers)
            continue
        ws_exist = wb[sname]
        exist_headers = [c.value for c in ws_exist[1] if c.value]
        for h in headers:
            if h not in exist_headers:
                ws_exist.cell(1, ws_exist.max_column + 1).value = h
                exist_headers.append(h)

    # Ensure admin
    ws = wb["users"]
    headers = [c.value for c in ws[1]]
    if "username" in headers:
        u_col = headers.index("username") + 1
        p_col = headers.index("password_hash") + 1
        r_col = headers.index("role") + 1

        admin_row = None
        for r_idx in range(2, ws.max_row + 1):
            if ws.cell(r_idx, u_col).value == "admin":
                admin_row = r_idx
                break
        
        default_hash = hash_pw(DEFAULT_ADMIN["password"])
        if admin_row is None:
            ws.append(["admin", default_hash, "admin", now_iso()])
        else:
            cur_hash = ws.cell(admin_row, p_col).value
            cur_role = (ws.cell(admin_row, r_col).value or "").lower()
            if not cur_hash or str(cur_hash).strip() == "" or cur_hash == "None":
                ws.cell(admin_row, p_col).value = default_hash
            if cur_role != "admin":
                ws.cell(admin_row, r_col).value = "admin"
        wb.save(DB_FILE)

    os.makedirs(BACKUP_DIR, exist_ok=True)

def get_wb() -> Workbook:
    ensure_db()
    return openpyxl.load_workbook(DB_FILE)

def read_rows(sheet: str) -> List[Dict[str, Any]]:
    wb = get_wb()
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    out: List[Dict[str, Any]] = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if all(v is None for v in row):
            continue
        out.append(dict(zip(headers, row)))
    return out

def append_row(sheet: str, row: Dict[str, Any]) -> None:
    wb = get_wb()
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    ws.append([row.get(h) for h in headers])
    wb.save(DB_FILE)

def next_id(sheet: str) -> int:
    rows = read_rows(sheet)
    mx = 0
    for r in rows:
        try:
            mx = max(mx, int(r.get("id") or 0))
        except Exception:
            pass
    return mx + 1

def update_row_by_id(sheet: str, row_id: int, updates: Dict[str, Any]) -> bool:
    wb = get_wb()
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    if "id" not in headers:
        return False
    id_col = headers.index("id") + 1
    for r_idx in range(2, ws.max_row + 1):
        if ws.cell(r_idx, id_col).value == row_id:
            for k, v in updates.items():
                if k in headers:
                    c_idx = headers.index(k) + 1
                    ws.cell(r_idx, c_idx).value = v
            wb.save(DB_FILE)
            return True
    return False

def delete_row_by_id(sheet: str, row_id: int) -> bool:
    wb = get_wb()
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    if "id" not in headers:
        return False
    id_col = headers.index("id") + 1
    for r_idx in range(2, ws.max_row + 1):
        if ws.cell(r_idx, id_col).value == row_id:
            ws.delete_rows(r_idx, 1)
            wb.save(DB_FILE)
            return True
    return False

def log_action(action: str, details: str = "") -> None:
    try:
        append_row("logs", {"timestamp": now_iso(), "action": action, "details": details})
    except:
        pass

# -------------------------------------------------
# Auth helpers
# -------------------------------------------------
def current_user() -> Optional[Dict[str, str]]:
    if "username" not in session:
        return None
    return {"username": session["username"], "role": session.get("role", "user")}

def require_login():
    if not current_user():
        return jsonify({"error": "login_required"}), 401
    return None

def require_admin():
    u = current_user()
    if not u:
        return jsonify({"error": "login_required"}), 401
    if u["role"] != "admin":
        return jsonify({"error": "admin_required"}), 403
    return None

def can_edit_row(row: Dict[str, Any]) -> bool:
    u = current_user()
    if not u:
        return False
    if u["role"] == "admin":
        return True
    return (row.get("created_by") or "") == u["username"]

# -------------------------------------------------
# FX helpers
# -------------------------------------------------
def load_fx_cache() -> Dict[str, Any]:
    if not os.path.exists(FX_CACHE_FILE):
        return {}
    try:
        with open(FX_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_fx_cache(cache: Dict[str, Any]) -> None:
    try:
        with open(FX_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f)
    except Exception:
        pass

def fetch_fx_rates(base: str = "USD") -> Dict[str, float]:
    cache = load_fx_cache()
    ts = cache.get("timestamp")
    if ts and cache.get("base") == base and cache.get("rates"):
        try:
            age = datetime.utcnow().timestamp() - float(ts)
            if age < FX_CACHE_TTL_SECONDS:
                return cache["rates"]
        except Exception:
            pass
    try:
        r = requests.get("https://api.exchangerate.host/latest", params={"base": base}, timeout=5)
        data = r.json()
        rates = data.get("rates") or {}
        if rates:
            save_fx_cache({"timestamp": datetime.utcnow().timestamp(), "base": base, "rates": rates})
            return rates
    except:
        pass
    if cache.get("rates"):
        return cache["rates"]
    return {"USD": 1.0, "EUR": 0.9, "AED": 3.67, "GBP": 0.78}

def to_usd(amount: float, currency: str) -> float:
    currency = (currency or "USD").upper()
    if currency == "USD": return float(amount)
    rates = fetch_fx_rates("USD")
    r = rates.get(currency)
    if not r or r == 0: return float(amount)
    return float(amount) / float(r)

# -------------------------------------------------
# Backup Logic (Smart Check)
# -------------------------------------------------
def make_backup_filename(suffix:str="") -> str:
    # microseconds included for uniqueness
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    return f"office_ops_backup_{ts}{suffix}.xlsx"

def create_backup_file(suffix:str="") -> str:
    ensure_db()
    os.makedirs(BACKUP_DIR, exist_ok=True)
    name = make_backup_filename(suffix)
    path = os.path.join(BACKUP_DIR, name)
    shutil.copy2(DB_FILE, path)
    
    # Cleanup: Keep only last 20 backups
    try:
        all_backups = sorted([os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.endswith(".xlsx")])
        while len(all_backups) > 20:
            os.remove(all_backups.pop(0))
    except:
        pass
        
    log_action("backup_create", name)
    return path

# Global variable to limit how often we check disk (don't check every millisecond)
LAST_AUTO_CHECK_TIME = 0

def check_and_run_smart_backup():
    """
    Checks if an auto-backup is needed. 
    1. If no AUTO backup exists -> create one.
    2. If last AUTO backup is older than 12 hours -> create one.
    """
    global LAST_AUTO_CHECK_TIME
    # Only check once every 5 minutes to avoid slowing down the server
    if time.time() - LAST_AUTO_CHECK_TIME < 300:
        return

    LAST_AUTO_CHECK_TIME = time.time()
    
    ensure_db()
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # Filter for auto backups only
    auto_backups = [f for f in os.listdir(BACKUP_DIR) if "_AUTO" in f and f.endswith(".xlsx")]
    
    should_backup = False
    
    if not auto_backups:
        # Case 1: No auto backups exist yet.
        should_backup = True
    else:
        # Case 2: Check if the newest one is too old
        try:
            auto_backups.sort(reverse=True) # newest first based on name timestamp
            newest_file = os.path.join(BACKUP_DIR, auto_backups[0])
            # Check file creation/mod time
            mtime = os.path.getmtime(newest_file)
            age_seconds = time.time() - mtime
            
            if age_seconds > AUTO_BACKUP_INTERVAL_SECONDS:
                should_backup = True
        except:
            # If error checking time, just be safe and backup
            should_backup = True
            
    if should_backup:
        print(f"[{datetime.utcnow()}] Triggering Smart Auto-Backup...")
        create_backup_file(suffix="_AUTO")

# -------------------------------------------------
# Middleware (The Smart Trigger)
# -------------------------------------------------
@app.before_request
def trigger_backup_check():
    """
    Before handling any request (like loading the page or logging in),
    check if we need to backup. This ensures backups happen even if
    the server slept for a week.
    """
    # Only trigger on API calls or HTML loads (ignore static assets like css/js to save speed)
    if request.path == "/" or request.path.startswith("/api/"):
        try:
            check_and_run_smart_backup()
        except:
            pass

# -------------------------------------------------
# Routes
# -------------------------------------------------
@app.get("/")
def home():
    return send_from_directory("static", "index.html")

@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "time": now_iso()})

# --- Auth ---
@app.post("/api/login")
def login():
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    users = read_rows("users")
    u = next((x for x in users if x.get("username") == username), None)
    if not u: return jsonify({"error": "invalid_credentials"}), 401
    stored_hash = (u.get("password_hash") or "").strip()
    if (not stored_hash) or (stored_hash != hash_pw(password)):
        return jsonify({"error": "invalid_credentials"}), 401
    session["username"] = username
    session["role"] = (u.get("role") or "user").lower()
    log_action("login", username)
    return jsonify({"ok": True, "username": username, "role": session["role"]})

@app.post("/api/logout")
def logout():
    session.clear()
    return jsonify({"ok": True})

@app.get("/api/session")
def session_info():
    u = current_user()
    if not u: return jsonify({"logged_in": False}), 401
    return jsonify({"logged_in": True, **u})

# --- FX ---
@app.get("/api/currencies")
def api_currencies():
    return jsonify({"currencies": sorted(set(fetch_fx_rates("USD").keys()) | {"USD"})})

@app.get("/api/fx")
def api_fx():
    return jsonify({"base": "USD", "rates": fetch_fx_rates("USD")})

# -------------------------------------------------
# Backup routes (ADMIN ONLY)
# -------------------------------------------------
@app.get("/api/backup")
def download_backup_legacy():
    return download_backup_direct()

@app.get("/api/backup/download")
def download_backup_direct():
    guard = require_admin()
    if guard: return guard
    path = create_backup_file(suffix="_MANUAL")
    return send_file(path, as_attachment=True, download_name=os.path.basename(path))

@app.post("/api/backup/create")
def create_backup_api():
    guard = require_admin()
    if guard: return guard
    create_backup_file(suffix="_MANUAL")
    return jsonify({"ok": True})

@app.get("/api/backups")
def api_list_backups():
    guard = require_admin()
    if guard: return guard
    ensure_db()
    os.makedirs(BACKUP_DIR, exist_ok=True)
    out = []
    for fn in sorted(os.listdir(BACKUP_DIR), reverse=True):
        if not fn.lower().endswith(".xlsx"): continue
        fp = os.path.join(BACKUP_DIR, fn)
        st = os.stat(fp)
        out.append({
            "name": fn,
            "size": st.st_size,
            "created_at": datetime.utcfromtimestamp(st.st_mtime).isoformat(timespec="seconds") + "Z"
        })
    return jsonify(out)

@app.get("/api/backups/<name>/download")
def api_download_backup(name: str):
    guard = require_admin()
    if guard: return guard
    fp = os.path.join(BACKUP_DIR, name)
    if not os.path.exists(fp): return jsonify({"error": "not_found"}), 404
    return send_file(fp, as_attachment=True, download_name=name)

@app.post("/api/backups/<name>/restore")
def api_restore_backup(name: str):
    guard = require_admin()
    if guard: return guard
    fp = os.path.join(BACKUP_DIR, name)
    if not os.path.exists(fp): return jsonify({"error": "not_found"}), 404
    shutil.copy2(fp, DB_FILE)
    log_action("backup_restore", name)
    return jsonify({"ok": True})

@app.delete("/api/backups/<name>")
def api_delete_backup(name: str):
    guard = require_admin()
    if guard: return guard
    fp = os.path.join(BACKUP_DIR, name)
    if not os.path.exists(fp): return jsonify({"error": "not_found"}), 404
    os.remove(fp)
    return jsonify({"ok": True})

@app.post("/api/upload")
def upload_overwrite_db():
    guard = require_admin()
    if guard: return guard
    if "file" not in request.files: return jsonify({"error": "missing_file"}), 400
    file = request.files["file"]
    filename = secure_filename(file.filename or "")
    if not filename.endswith(".xlsx"): return jsonify({"error": "invalid_file_type"}), 400
    tmp_path = os.path.join(BASE_DIR, f"_upload_tmp.xlsx")
    file.save(tmp_path)
    try:
        openpyxl.load_workbook(tmp_path)
    except:
        os.remove(tmp_path)
        return jsonify({"error": "corrupt_excel"}), 400
    shutil.copy2(tmp_path, DB_FILE)
    os.remove(tmp_path)
    ensure_db()
    return jsonify({"ok": True})

# -------------------------------------------------
# Categories (admin)
# -------------------------------------------------
@app.get("/api/categories")
def get_categories(): return jsonify(read_rows("categories"))
@app.post("/api/categories")
def add_category():
    guard = require_admin()
    if guard: return guard
    data = request.json or {}
    name = (data.get("name") or "").strip()
    abbr = (data.get("abbr") or "").strip().upper()
    if not name or not abbr: return jsonify({"error": "missing_fields"}), 400
    cats = read_rows("categories")
    if any((c.get("abbr") or "").upper() == abbr for c in cats): return jsonify({"error": "duplicate_abbr"}), 409
    row = {"id": next_id("categories"),"name": name,"abbr": abbr,"created_at": now_iso()}
    append_row("categories", row)
    return jsonify(row)
@app.patch("/api/categories/<int:cid>")
def edit_category(cid: int):
    guard = require_admin()
    if guard: return guard
    data=request.json or {}
    if not update_row_by_id("categories", cid, data): return jsonify({"error": "not_found"}), 404
    return jsonify({"ok": True})
@app.delete("/api/categories/<int:cid>")
def delete_category(cid: int):
    guard = require_admin()
    if guard: return guard
    if not delete_row_by_id("categories", cid): return jsonify({"error": "not_found"}), 404
    return jsonify({"ok": True})

# -------------------------------------------------
# Vessels
# -------------------------------------------------
@app.get("/api/vessels")
def get_vessels(): return jsonify(read_rows("vessels"))
@app.post("/api/vessels")
def add_vessel():
    guard = require_admin()
    if guard: return guard
    name = (request.json or {}).get("name","").strip()
    if not name: return jsonify({"error": "missing_fields"}), 400
    vs = read_rows("vessels")
    if any((v.get("name") or "").strip().lower() == name.lower() for v in vs): return jsonify({"error": "duplicate_name"}), 409
    row = {"id": next_id("vessels"), "name": name, "created_at": now_iso()}
    append_row("vessels", row)
    return jsonify(row)
@app.patch("/api/vessels/<int:vid>")
def edit_vessel(vid: int):
    guard = require_admin()
    if guard: return guard
    name=(request.json or {}).get("name","").strip()
    if not update_row_by_id("vessels", vid, {"name":name}): return jsonify({"error": "not_found"}), 404
    return jsonify({"ok": True})
@app.delete("/api/vessels/<int:vid>")
def delete_vessel(vid: int):
    guard = require_admin()
    if guard: return guard
    if not delete_row_by_id("vessels", vid): return jsonify({"error": "not_found"}), 404
    return jsonify({"ok": True})

# -------------------------------------------------
# Users, Directory, Requisitions, Landings
# -------------------------------------------------
@app.get("/api/users")
def list_users():
    if require_admin(): return require_admin()
    return jsonify(read_rows("users"))
@app.post("/api/users")
def add_user():
    if require_admin(): return require_admin()
    data=request.json
    append_row("users", {"username":data["username"],"password_hash":hash_pw(data["password"]),"role":data["role"],"created_at":now_iso()})
    return jsonify({"ok":True})
@app.delete("/api/users/<username>")
def delete_user_api(username):
    if require_admin(): return require_admin()
    if username=="admin": return jsonify({"error":"cannot_delete_admin"}),400
    wb=get_wb(); ws=wb["users"]; u_col=1
    for r in range(2,ws.max_row+1):
        if ws.cell(r,u_col).value==username: ws.delete_rows(r,1); wb.save(DB_FILE); return jsonify({"ok":True})
    return jsonify({"error":"not_found"}),404

@app.get("/api/requisitions")
def list_requisitions():
    rows=read_rows("requisitions")
    for r in rows: r["total_amount"]=float(r.get("amount_usd")or 0)
    return jsonify(rows)
@app.post("/api/requisitions")
def add_requisition():
    if require_login(): return require_login()
    d=request.json
    row={**d, "id":next_id("requisitions"), "created_by":current_user()["username"], "created_at":now_iso()}
    row["amount_usd"] = round(to_usd(float(d.get("amount") or 0), d.get("currency")),2)
    append_row("requisitions", row)
    return jsonify(row)
@app.patch("/api/requisitions/<int:rid>")
def edit_requisition(rid):
    if require_login(): return require_login()
    d=request.json
    if "amount" in d: d["amount_usd"] = round(to_usd(float(d["amount"]), d.get("currency","USD")),2)
    if update_row_by_id("requisitions", rid, d): return jsonify({"ok":True})
    return jsonify({"error":"not_found"}),404
@app.patch("/api/requisitions/<int:rid>/toggle_paid")
def toggle_paid_req(rid):
    if require_login(): return require_login()
    rows=read_rows("requisitions")
    row=next((r for r in rows if int(r["id"])==rid),None)
    new_p = 0 if int(row.get("paid")or 0) else 1
    update_row_by_id("requisitions", rid, {"paid":new_p})
    return jsonify({"ok":True})
@app.delete("/api/requisitions/<int:rid>")
def delete_req(rid):
    if require_admin(): return require_admin()
    delete_row_by_id("requisitions", rid)
    return jsonify({"ok":True})

@app.get("/api/landings")
def list_landings():
    rows=read_rows("landings")
    for r in rows: r["amount"]=float(r.get("amount_usd")or 0)
    return jsonify(rows)
@app.post("/api/landings")
def add_landing():
    if require_login(): return require_login()
    d=request.json
    row={**d, "id":next_id("landings"), "created_by":current_user()["username"], "created_at":now_iso()}
    row["amount_usd"] = round(to_usd(float(d.get("amount") or 0), d.get("currency")),2)
    append_row("landings", row)
    return jsonify(row)
@app.patch("/api/landings/<int:lid>")
def edit_landing(lid):
    if require_login(): return require_login()
    d=request.json
    if "amount" in d: d["amount_usd"] = round(to_usd(float(d["amount"]), d.get("currency","USD")),2)
    update_row_by_id("landings", lid, d)
    return jsonify({"ok":True})
@app.patch("/api/landings/<int:lid>/toggle_paid")
def toggle_paid_land(lid):
    if require_login(): return require_login()
    rows=read_rows("landings")
    row=next((r for r in rows if int(r["id"])==lid),None)
    new_p = 0 if int(row.get("paid")or 0) else 1
    update_row_by_id("landings", lid, {"paid":new_p})
    return jsonify({"ok":True})
@app.delete("/api/landings/<int:lid>")
def delete_land(lid):
    if require_admin(): return require_admin()
    delete_row_by_id("landings", lid)
    return jsonify({"ok":True})

@app.get("/api/directory")
def get_dir(): return jsonify(read_rows("directory"))
@app.post("/api/directory")
def add_directory_std():
    if require_login(): return require_login()
    d=request.json
    row={**d, "id":next_id("directory"), "created_by":current_user()["username"], "created_at":now_iso()}
    append_row("directory", row)
    return jsonify(row)
@app.post("/api/directory/quick")
def add_dir_quick():
    return add_directory_std()
@app.patch("/api/directory/<int:did>")
def edit_directory_entry(did: int):
    if require_login(): return require_login()
    update_row_by_id("directory", did, request.json or {})
    return jsonify({"ok": True})
@app.delete("/api/directory/<int:did>")
def del_dir(did):
    if require_admin(): return require_admin()
    delete_row_by_id("directory", did)
    return jsonify({"ok":True})

# -------------------------------------------------
# Run
# -------------------------------------------------
if __name__ == "__main__":
    ensure_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
