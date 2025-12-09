"""
AMT Procurement - Single-file Flask Backend (Excel DB)
FIXED: Document Uploads, Vendor Score, Aging Report, PO Gen, Dubai Time
"""

import os
import json
import hashlib
import shutil
import time
from datetime import datetime, timedelta
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
UPLOAD_FOLDER = os.path.join(BASE_DIR, "static", "uploads")

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
        "po_number", "remarks", "urgency", "tracking_url",
        "created_by", "created_at", "updated_at", "delivery_status"
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
        "created_by", "created_at", "rating", "rating_comment"
    ],
    "categories": ["id", "name", "abbr", "created_at"],
    "vessels": ["id", "name", "created_at"],
    "logs": ["timestamp", "user", "action", "target", "details"],
    "documents": ["id", "parent_type", "parent_id", "filename", "uploaded_at", "uploaded_by"]
}

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def get_dubai_time():
    return datetime.utcnow() + timedelta(hours=4)

def now_iso() -> str:
    return get_dubai_time().isoformat(timespec="seconds")

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

def ensure_db() -> None:
    # 1. Create file if missing
    if not os.path.exists(DB_FILE):
        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            wb.remove(wb["Sheet"])
        for sname, headers in SHEETS.items():
            ws = wb.create_sheet(sname)
            ws.append(headers)
        wb.save(DB_FILE)

    # 2. Load and migrate sheets
    try:
        wb = openpyxl.load_workbook(DB_FILE)
        modified = False
        
        for sname, headers in SHEETS.items():
            if sname not in wb.sheetnames:
                ws_new = wb.create_sheet(sname)
                ws_new.append(headers)
                modified = True
                continue
            
            ws_exist = wb[sname]
            exist_headers = [c.value for c in ws_exist[1] if c.value]
            for h in headers:
                if h not in exist_headers:
                    ws_exist.cell(1, ws_exist.max_column + 1).value = h
                    exist_headers.append(h)
                    modified = True
        
        # Admin check
        ws = wb["users"]
        headers = [c.value for c in ws[1]]
        if "username" in headers:
            u_col = headers.index("username") + 1
            found_admin = False
            for r in range(2, ws.max_row + 1):
                if ws.cell(r, u_col).value == "admin":
                    found_admin = True
                    break
            if not found_admin:
                default_hash = hash_pw(DEFAULT_ADMIN["password"])
                ws.append(["admin", default_hash, "admin", now_iso()])
                modified = True

        if modified:
            wb.save(DB_FILE)
            
    except Exception as e:
        print(f"DB Init Error: {e}")

    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def get_wb() -> Workbook:
    ensure_db()
    return openpyxl.load_workbook(DB_FILE)

def read_rows(sheet: str) -> List[Dict[str, Any]]:
    wb = get_wb()
    if sheet not in wb.sheetnames: return []
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    out: List[Dict[str, Any]] = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if all(v is None for v in row): continue
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
        except: pass
    return mx + 1

def update_row_by_id(sheet: str, row_id: int, updates: Dict[str, Any]) -> bool:
    wb = get_wb()
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    if "id" not in headers: return False
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
    if "id" not in headers: return False
    id_col = headers.index("id") + 1
    for r_idx in range(2, ws.max_row + 1):
        if ws.cell(r_idx, id_col).value == row_id:
            ws.delete_rows(r_idx, 1)
            wb.save(DB_FILE)
            return True
    return False

# -------------------------------------------------
# Auth
# -------------------------------------------------
def current_user() -> Optional[Dict[str, str]]:
    if "username" not in session: return None
    return {"username": session["username"], "role": session.get("role", "user")}

def log_action(action: str, target: str = "", details: str = "") -> None:
    try:
        u = current_user()
        username = u["username"] if u else "system"
        append_row("logs", {
            "timestamp": now_iso(), "user": username,
            "action": action, "target": target, "details": details
        })
    except: pass

def require_login():
    if not current_user(): return jsonify({"error": "login_required"}), 401
    return None

def require_admin():
    u = current_user()
    if not u: return jsonify({"error": "login_required"}), 401
    if u["role"] != "admin": return jsonify({"error": "admin_required"}), 403
    return None

# -------------------------------------------------
# FX helpers
# -------------------------------------------------
def load_fx_cache() -> Dict[str, Any]:
    if not os.path.exists(FX_CACHE_FILE): return {}
    try:
        with open(FX_CACHE_FILE, "r") as f: return json.load(f)
    except: return {}

def save_fx_cache(cache: Dict[str, Any]) -> None:
    try:
        with open(FX_CACHE_FILE, "w") as f: json.dump(cache, f)
    except: pass

def fetch_fx_rates(base: str = "USD") -> Dict[str, float]:
    cache = load_fx_cache()
    ts = cache.get("timestamp")
    if ts and cache.get("base") == base and cache.get("rates"):
        if datetime.utcnow().timestamp() - float(ts) < FX_CACHE_TTL_SECONDS:
            return cache["rates"]
    try:
        r = requests.get("https://api.exchangerate.host/latest", params={"base": base}, timeout=5)
        rates = r.json().get("rates")
        if rates:
            save_fx_cache({"timestamp": datetime.utcnow().timestamp(), "base": base, "rates": rates})
            return rates
    except: pass
    if cache.get("rates"): return cache["rates"]
    return {"USD": 1.0, "EUR": 0.9, "AED": 3.67, "GBP": 0.78}

def to_usd(amount: float, currency: str) -> float:
    currency = (currency or "USD").upper()
    if currency == "USD": return float(amount)
    rates = fetch_fx_rates("USD")
    r = rates.get(currency)
    if not r: return float(amount)
    return float(amount) / float(r)

# -------------------------------------------------
# Backup
# -------------------------------------------------
def create_backup_file(suffix:str="") -> str:
    ensure_db()
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = get_dubai_time().strftime("%Y%m%d_%H%M%S_%f")
    name = f"office_ops_backup_{ts}{suffix}.xlsx"
    path = os.path.join(BACKUP_DIR, name)
    shutil.copy2(DB_FILE, path)
    log_action("Backup", target=name)
    return path

LAST_AUTO_CHECK_TIME = 0
def check_smart_backup():
    global LAST_AUTO_CHECK_TIME
    if time.time() - LAST_AUTO_CHECK_TIME < 300: return
    LAST_AUTO_CHECK_TIME = time.time()
    ensure_db()
    try:
        backups = sorted([f for f in os.listdir(BACKUP_DIR) if "_AUTO" in f], reverse=True)
        if not backups:
            create_backup_file("_AUTO")
        else:
            last_path = os.path.join(BACKUP_DIR, backups[0])
            if time.time() - os.path.getmtime(last_path) > AUTO_BACKUP_INTERVAL_SECONDS:
                create_backup_file("_AUTO")
    except: pass

@app.before_request
def trigger_backup():
    if request.path == "/" or request.path.startswith("/api/"):
        check_smart_backup()

# -------------------------------------------------
# Routes
# -------------------------------------------------
@app.get("/")
def home(): return send_from_directory("static", "index.html")

@app.get("/api/health")
def health(): return jsonify({"status": "ok", "time": now_iso()})

@app.post("/api/login")
def login():
    d = request.json or {}
    u = next((x for x in read_rows("users") if x["username"] == d.get("username")), None)
    if u and u["password_hash"] == hash_pw(d.get("password") or ""):
        session["username"] = u["username"]
        session["role"] = u.get("role", "user")
        log_action("Login", target=u["username"])
        return jsonify({"ok": True, "username": u["username"], "role": session["role"]})
    return jsonify({"error": "invalid"}), 401

@app.post("/api/logout")
def logout(): session.clear(); return jsonify({"ok": True})

@app.get("/api/session")
def get_session():
    u = current_user()
    return jsonify({"logged_in": bool(u), **(u or {})})

@app.get("/api/audit")
def audit_log():
    if require_admin(): return require_admin()
    return jsonify(sorted(read_rows("logs"), key=lambda x: x.get("timestamp") or "", reverse=True))

@app.get("/api/vendors/<name>/score")
def vendor_score(name):
    reqs = [r for r in read_rows("requisitions") if r.get("supplier") == name]
    if not reqs: return jsonify({"score": 0, "total": 0})
    # Score logic: Delivered = 1pt, Paid = 1pt. Normalized to 5.
    pts = 0
    for r in reqs:
        if str(r.get("delivered")) == "1": pts += 1
        if str(r.get("paid")) == "1": pts += 1
    max_pts = len(reqs) * 2
    score = (pts / max_pts) * 5 if max_pts > 0 else 0
    return jsonify({"score": round(max(1.0, min(5.0, score)), 1), "total": len(reqs)})

@app.get("/api/reports/aging")
def aging_report():
    now = get_dubai_time()
    out = []
    for r in read_rows("requisitions"):
        if str(r.get("paid")) != "1" and (r.get("status") or "").lower() != "cancelled":
            try:
                dt = datetime.strptime(str(r.get("date_ordered")), "%Y-%m-%d")
                days = (now - dt).days
                grp = "<30"
                if days > 90: grp = ">90"
                elif days > 60: grp = "60-90"
                elif days > 30: grp = "30-60"
                out.append({"po": r.get("po_number"), "supplier": r.get("supplier"), 
                           "amount": r.get("amount_usd"), "days": days, "group": grp})
            except: pass
    return jsonify(out)

# --- DOCUMENTS ---
@app.post("/api/documents/upload")
def upload_doc():
    if require_login(): return require_login()
    if "file" not in request.files: return jsonify({"error": "no_file"}), 400
    f = request.files["file"]
    ptype = request.form.get("parent_type")
    pid = request.form.get("parent_id")
    if not f or not pid: return jsonify({"error": "missing_data"}), 400
    
    fname = secure_filename(f.filename)
    sname = f"{ptype}_{pid}_{int(time.time())}_{fname}"
    f.save(os.path.join(UPLOAD_FOLDER, sname))
    
    append_row("documents", {
        "id": next_id("documents"), "parent_type": ptype, "parent_id": pid,
        "filename": sname, "uploaded_at": now_iso(), "uploaded_by": current_user()["username"]
    })
    log_action("Upload", target=fname)
    return jsonify({"ok": True})

@app.get("/api/documents/<ptype>/<pid>")
def list_docs(ptype, pid):
    if require_login(): return require_login()
    docs = [d for d in read_rows("documents") if str(d.get("parent_type"))==str(ptype) and str(d.get("parent_id"))==str(pid)]
    return jsonify(docs)

# --- Standard CRUD (Req, Land, Dir, Cats, Ves) ---
@app.get("/api/requisitions")
def get_reqs(): return jsonify(read_rows("requisitions"))
@app.post("/api/requisitions")
def add_req():
    if require_login(): return require_login()
    d = request.json
    row = {**d, "id": next_id("requisitions"), "created_at": now_iso(), "created_by": current_user()["username"]}
    row["paid"] = 1 if d.get("paid") else 0
    row["delivered"] = int(d.get("delivered") or 0)
    row["amount_usd"] = round(to_usd(float(d.get("amount") or 0), d.get("currency")), 2)
    # Ensure original amount is saved
    if "amount_original" not in row or not row["amount_original"]:
        row["amount_original"] = d.get("amount")
    
    append_row("requisitions", row)
    log_action("Add Req", target=str(row.get("po_number")))
    return jsonify(row)
@app.patch("/api/requisitions/<int:rid>")
def edit_req(rid):
    if require_login(): return require_login()
    d = request.json
    if "paid" in d: d["paid"] = 1 if d.get("paid") else 0
    if "amount" in d: d["amount_usd"] = round(to_usd(float(d["amount"]), d.get("currency","USD")), 2)
    update_row_by_id("requisitions", rid, d)
    return jsonify({"ok": True})
@app.delete("/api/requisitions/<int:rid>")
def del_req(rid):
    if require_admin(): return require_admin()
    delete_row_by_id("requisitions", rid)
    return jsonify({"ok": True})
@app.post("/api/requisitions/bulk")
def bulk_req():
    if require_login(): return require_login()
    d = request.json
    upd = {}
    act = d.get("action")
    if act == "mark_paid": upd = {"paid": 1}
    elif act == "mark_unpaid": upd = {"paid": 0}
    elif act == "mark_delivered": upd = {"delivered": 1}
    elif act == "mark_partial": upd = {"delivered": 2}
    
    cnt = 0
    for i in d.get("ids", []):
        if update_row_by_id("requisitions", int(i), upd): cnt += 1
    log_action("Bulk Req", details=f"{act} {cnt}")
    return jsonify({"ok": True, "updated": cnt})

@app.get("/api/landings")
def get_lands(): return jsonify(read_rows("landings"))
@app.post("/api/landings")
def add_land():
    if require_login(): return require_login()
    d=request.json
    row = {**d, "id": next_id("landings"), "created_at": now_iso(), "created_by": current_user()["username"]}
    row["paid"] = 1 if d.get("paid") else 0
    row["amount_usd"] = round(to_usd(float(d.get("amount") or 0), d.get("currency")), 2)
    if "amount_original" not in row: row["amount_original"] = d.get("amount")
    append_row("landings", row)
    return jsonify(row)
@app.patch("/api/landings/<int:lid>")
def edit_land(lid):
    if require_login(): return require_login()
    d = request.json
    if "paid" in d: d["paid"] = 1 if d.get("paid") else 0
    if "amount" in d: d["amount_usd"] = round(to_usd(float(d["amount"]), d.get("currency","USD")), 2)
    update_row_by_id("landings", lid, d)
    return jsonify({"ok": True})
@app.delete("/api/landings/<int:lid>")
def del_land(lid):
    if require_admin(): return require_admin()
    delete_row_by_id("landings", lid)
    return jsonify({"ok": True})
@app.get("/api/backup/download")
def download_backup_direct():
    if require_admin(): return require_admin()
    # Create a fresh backup immediately and send it
    path = create_backup_file(suffix="_MANUAL")
    return send_file(path, as_attachment=True, download_name=os.path.basename(path))
@app.post("/api/landings/bulk")
def bulk_land():
    if require_login(): return require_login()
    d = request.json
    upd = {}
    act = d.get("action")
    if act == "mark_paid": upd = {"paid": 1}
    elif act == "mark_unpaid": upd = {"paid": 0}
    elif act == "mark_delivered": upd = {"delivered": 1}
    elif act == "mark_partial": upd = {"delivered": 2}
    for i in d.get("ids", []): update_row_by_id("landings", int(i), upd)
    return jsonify({"ok": True})

@app.get("/api/directory")
def get_dirs(): return jsonify(read_rows("directory"))
@app.post("/api/directory")
def add_dir():
    if require_login(): return require_login()
    d=request.json
    row = {**d, "id": next_id("directory"), "created_at": now_iso()}
    append_row("directory", row)
    return jsonify(row)
@app.post("/api/directory/quick")
def quick_dir(): return add_dir()
@app.patch("/api/directory/<int:did>")
def edit_dir(did):
    if require_login(): return require_login()
    update_row_by_id("directory", did, request.json)
    return jsonify({"ok": True})
@app.delete("/api/directory/<int:did>")
def del_dir(did):
    if require_admin(): return require_admin()
    delete_row_by_id("directory", did)
    return jsonify({"ok": True})

@app.get("/api/categories")
def get_cats(): return jsonify(read_rows("categories"))
@app.post("/api/categories")
def add_cat():
    if require_admin(): return require_admin()
    d=request.json
    append_row("categories", {**d, "id": next_id("categories")})
    return jsonify({"ok": True})
@app.delete("/api/categories/<int:cid>")
def del_cat(cid):
    if require_admin(): return require_admin()
    delete_row_by_id("categories", cid)
    return jsonify({"ok": True})

@app.get("/api/vessels")
def get_ves(): return jsonify(read_rows("vessels"))
@app.post("/api/vessels")
def add_ves():
    if require_admin(): return require_admin()
    d=request.json
    append_row("vessels", {**d, "id": next_id("vessels")})
    return jsonify({"ok": True})
@app.delete("/api/vessels/<int:vid>")
def del_ves(vid):
    if require_admin(): return require_admin()
    delete_row_by_id("vessels", vid)
    return jsonify({"ok": True})

@app.get("/api/users")
def get_users():
    if require_admin(): return require_admin()
    return jsonify(read_rows("users"))
@app.post("/api/users")
def add_user():
    if require_admin(): return require_admin()
    d=request.json
    append_row("users", {"username":d["username"], "password_hash":hash_pw(d["password"]), "role":d["role"]})
    return jsonify({"ok": True})
@app.delete("/api/users/<username>")
def del_user(username):
    if require_admin(): return require_admin()
    wb = get_wb(); ws = wb["users"]
    for r in range(2, ws.max_row+1):
        if ws.cell(r, 1).value == username:
            ws.delete_rows(r, 1)
            wb.save(DB_FILE)
            return jsonify({"ok": True})
    return jsonify({"error": "not found"}), 404

# --- FX/Backup ---
@app.get("/api/currencies")
def currs(): return jsonify({"currencies": ["USD","EUR","GBP","AED","SGD","JPY"]})
@app.get("/api/fx")
def fx(): return jsonify({"base":"USD", "rates": fetch_fx_rates()})
@app.get("/api/backups")
def get_bks():
    ensure_db()
    out = []
    for f in sorted(os.listdir(BACKUP_DIR), reverse=True):
        if f.endswith(".xlsx"):
            st = os.stat(os.path.join(BACKUP_DIR, f))
            out.append({"name": f, "size": st.st_size, "created_at": (datetime.utcfromtimestamp(st.st_mtime)+timedelta(hours=4)).isoformat()})
    return jsonify(out)
@app.post("/api/backup/create")
def make_bk():
    if require_admin(): return require_admin()
    create_backup_file("_MANUAL")
    return jsonify({"ok": True})
@app.get("/api/backups/<name>/download")
def dl_bk(name):
    if require_admin(): return require_admin()
    return send_file(os.path.join(BACKUP_DIR, name), as_attachment=True)
@app.post("/api/backups/<name>/restore")
def rest_bk(name):
    if require_admin(): return require_admin()
    shutil.copy2(os.path.join(BACKUP_DIR, name), DB_FILE)
    return jsonify({"ok": True})
@app.delete("/api/backups/<name>")
def del_bk(name):
    if require_admin(): return require_admin()
    os.remove(os.path.join(BACKUP_DIR, name))
    return jsonify({"ok": True})
@app.post("/api/upload")
def up_db():
    if require_admin(): return require_admin()
    f = request.files["file"]
    f.save(DB_FILE)
    ensure_db()
    return jsonify({"ok": True})

if __name__ == "__main__":
    ensure_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
