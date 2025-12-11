"""
AMT Procurement - Single-file Flask Backend (Excel DB)
UPDATED: Aging Report, Audit Logs, Restore/Upload, Partial Delivery
"""

import os
import json
import hashlib
import shutil
import time
import csv
import io
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
import openpyxl
from openpyxl import Workbook
from flask import Flask, jsonify, request, session, send_from_directory, send_file, make_response
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
# WARNING: On Render/Heroku, this file is ephemeral. Use a Persistent Disk for safety.
DB_FILE = os.path.join(BASE_DIR, "office_ops.xlsx")
UPLOAD_FOLDER = os.path.join(BASE_DIR, "static", "uploads")

DEFAULT_ADMIN = {"username": "admin", "password": "admin123", "role": "admin"}
DEFAULT_FINANCE = {"username": "finance", "password": "finance123", "role": "finance"}
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
        "rating", "rating_comment",
        "created_by", "created_at"
    ],
    "categories": ["id", "name", "abbr", "created_at"],
    "vessels": ["id", "name", "created_at"],
    "logs": ["timestamp", "user", "action", "target", "details"],
    "documents": ["id", "parent_type", "parent_id", "filename", "uploaded_at", "uploaded_by"]
}

# -------------------------------------------------
# Helpers (Dubai Time GMT+4)
# -------------------------------------------------
def get_dubai_time():
    # UTC + 4 hours
    return datetime.utcnow() + timedelta(hours=4)

def now_iso() -> str:
    return get_dubai_time().isoformat(timespec="seconds")

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

    # Ensure admin and finance users
    ws = wb["users"]
    headers = [c.value for c in ws[1]]
    if "username" in headers:
        u_col = headers.index("username") + 1
        p_col = headers.index("password_hash") + 1
        r_col = headers.index("role") + 1

        # Check/Create Admin
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

        # Check/Create Finance
        finance_row = None
        for r_idx in range(2, ws.max_row + 1):
            if ws.cell(r_idx, u_col).value == "finance":
                finance_row = r_idx
                break
        
        finance_hash = hash_pw(DEFAULT_FINANCE["password"])
        if finance_row is None:
            ws.append(["finance", finance_hash, "finance", now_iso()])

        wb.save(DB_FILE)

    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

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

# -------------------------------------------------
# Auth helpers & Logging
# -------------------------------------------------
def current_user() -> Optional[Dict[str, str]]:
    if "username" not in session:
        return None
    return {"username": session["username"], "role": session.get("role", "user")}

def log_action(action: str, target: str = "", details: str = "") -> None:
    try:
        u = current_user()
        username = u["username"] if u else "system"
        append_row("logs", {
            "timestamp": now_iso(),
            "user": username,
            "action": action,
            "target": target,
            "details": details
        })
    except:
        pass

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

def require_write_access():
    u = current_user()
    if not u:
        return jsonify({"error": "login_required"}), 401
    if u["role"] == "finance":
        return jsonify({"error": "read_only"}), 403
    return None

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
# Backup Logic (Optimized)
# -------------------------------------------------
def make_backup_filename(suffix:str="") -> str:
    ts = get_dubai_time().strftime("%Y%m%d_%H%M%S")
    return f"office_ops_backup_{ts}{suffix}.xlsx"

def create_backup_file(suffix:str="") -> str:
    ensure_db()
    os.makedirs(BACKUP_DIR, exist_ok=True)
    name = make_backup_filename(suffix)
    path = os.path.join(BACKUP_DIR, name)
    shutil.copy2(DB_FILE, path)
    
    try:
        all_backups = sorted([os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.endswith(".xlsx")])
        while len(all_backups) > 20:
            os.remove(all_backups.pop(0))
    except:
        pass
    return path

LAST_AUTO_CHECK_TIME = 0

def check_and_run_smart_backup():
    # PERFORMANCE FIX: Don't check filesystem on every request
    global LAST_AUTO_CHECK_TIME
    if time.time() - LAST_AUTO_CHECK_TIME < 300: # 5 Minutes
        return

    LAST_AUTO_CHECK_TIME = time.time()
    ensure_db()
    os.makedirs(BACKUP_DIR, exist_ok=True)

    try:
        auto_backups = [f for f in os.listdir(BACKUP_DIR) if "_AUTO" in f and f.endswith(".xlsx")]
        should_backup = False
        
        if not auto_backups:
            should_backup = True
        else:
            auto_backups.sort(reverse=True)
            newest_file = os.path.join(BACKUP_DIR, auto_backups[0])
            mtime = os.path.getmtime(newest_file)
            age_seconds = time.time() - mtime
            if age_seconds > AUTO_BACKUP_INTERVAL_SECONDS:
                should_backup = True
                
        if should_backup:
            create_backup_file(suffix="_AUTO")
    except Exception:
        pass

@app.before_request
def trigger_backup_check():
    if request.path == "/" or request.path.startswith("/api/"):
        check_and_run_smart_backup()

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
    log_action("Login", target=username)
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

# --- Activity Log ---
@app.get("/api/audit")
def get_audit_log():
    if require_admin(): return require_admin()
    rows = read_rows("logs")
    rows.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    
    # Return last 500 logs to prevent overload
    out = []
    for r in rows[:500]:
        out.append({
            "user": r.get("user") or "system",
            "action": r.get("action"),
            "target": r.get("target") or "",
            "details": r.get("details") or "",
            "date": r.get("timestamp")
        })
    return jsonify(out)

# --- Aging Report ---
@app.get("/api/reports/aging")
def get_aging_report():
    if require_login(): return require_login()
    reqs = read_rows("requisitions")
    now = get_dubai_time()
    
    unpaid = []
    for r in reqs:
        # Check if NOT paid and NOT cancelled
        status = (r.get("status") or "open").lower()
        paid_val = r.get("paid")
        is_paid = paid_val in [1, "1", True, "true"]
        
        if not is_paid and status != "cancelled":
            date_ord = r.get("date_ordered")
            if not date_ord: continue
            
            try:
                d_obj = datetime.strptime(str(date_ord), "%Y-%m-%d")
                delta = (now - d_obj).days
            except:
                delta = 0
            
            # Categorize
            group = "< 30 Days"
            if delta > 90: group = "> 90 Days"
            elif delta > 60: group = "60-90 Days"
            elif delta > 30: group = "30-60 Days"
            
            unpaid.append({
                "po": r.get("po_number") or r.get("number"),
                "supplier": r.get("supplier"),
                "amount": r.get("amount_usd"),
                "days": delta,
                "group": group
            })
            
    # Sort by oldest first
    unpaid.sort(key=lambda x: x["days"], reverse=True)
    return jsonify(unpaid)

# --- Document Uploads ---
@app.post("/api/documents/upload")
def upload_order_doc():
    if require_write_access(): return require_write_access()
    if "file" not in request.files: return jsonify({"error": "missing_file"}), 400
    
    file = request.files["file"]
    parent_type = request.form.get("parent_type", "req")
    parent_id = request.form.get("parent_id")
    
    if not file or not parent_id: return jsonify({"error": "missing_data"}), 400
    
    filename = secure_filename(file.filename)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    save_name = f"{parent_type}_{parent_id}_{ts}_{filename}"
    file.save(os.path.join(UPLOAD_FOLDER, save_name))
    
    # Save metadata
    row = {
        "id": next_id("documents"),
        "parent_type": parent_type,
        "parent_id": parent_id,
        "filename": save_name,
        "uploaded_at": now_iso(),
        "uploaded_by": current_user()["username"]
    }
    append_row("documents", row)
    log_action("Upload Doc", target=f"{parent_type} {parent_id}", details=filename)
    return jsonify({"ok": True})

@app.get("/api/documents/<ptype>/<pid>")
def list_documents(ptype, pid):
    if require_login(): return require_login()
    docs = read_rows("documents")
    filtered = [d for d in docs if str(d.get("parent_type")) == str(ptype) and str(d.get("parent_id")) == str(pid)]
    return jsonify(filtered)

# --- FX & Backup Routes ---
@app.get("/api/currencies")
def api_currencies():
    return jsonify({"currencies": sorted(set(fetch_fx_rates("USD").keys()) | {"USD"})})

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
        dt_dubai = datetime.utcfromtimestamp(st.st_mtime) + timedelta(hours=4)
        out.append({
            "name": fn,
            "size": st.st_size,
            "created_at": dt_dubai.isoformat(timespec="seconds")
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
    log_action("Restore Backup", target=name)
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
    
    # Save temp and verify
    tmp_path = os.path.join(BASE_DIR, f"_upload_tmp.xlsx")
    file.save(tmp_path)
    try:
        openpyxl.load_workbook(tmp_path)
    except:
        os.remove(tmp_path)
        return jsonify({"error": "corrupt_excel"}), 400
    
    # Overwrite
    shutil.copy2(tmp_path, DB_FILE)
    os.remove(tmp_path)
    ensure_db()
    log_action("Upload DB", details="Overwrote DB via upload")
    return jsonify({"ok": True})

# -------------------------------------------------
# Categories & Vessels
# -------------------------------------------------
@app.get("/api/categories")
def get_categories(): return jsonify(read_rows("categories"))
@app.post("/api/categories")
def add_category():
    if require_admin(): return require_admin()
    data = request.json or {}
    name = (data.get("name") or "").strip()
    abbr = (data.get("abbr") or "").strip().upper()
    if not name or not abbr: return jsonify({"error": "missing_fields"}), 400
    cats = read_rows("categories")
    if any((c.get("abbr") or "").upper() == abbr for c in cats): return jsonify({"error": "duplicate_abbr"}), 409
    row = {"id": next_id("categories"),"name": name,"abbr": abbr,"created_at": now_iso()}
    append_row("categories", row)
    return jsonify(row)

@app.delete("/api/categories/<int:cid>")
def delete_category(cid: int):
    if require_admin(): return require_admin()
    delete_row_by_id("categories", cid)
    return jsonify({"ok": True})

@app.get("/api/vessels")
def get_vessels(): return jsonify(read_rows("vessels"))
@app.post("/api/vessels")
def add_vessel():
    if require_admin(): return require_admin()
    name = (request.json or {}).get("name","").strip()
    if not name: return jsonify({"error": "missing_fields"}), 400
    row = {"id": next_id("vessels"), "name": name, "created_at": now_iso()}
    append_row("vessels", row)
    return jsonify(row)

@app.delete("/api/vessels/<int:vid>")
def delete_vessel(vid: int):
    if require_admin(): return require_admin()
    delete_row_by_id("vessels", vid)
    return jsonify({"ok": True})

# -------------------------------------------------
# Users, Directory
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
        if ws.cell(r,u_col).value==username: 
            ws.delete_rows(r,1); wb.save(DB_FILE)
            return jsonify({"ok":True})
    return jsonify({"error":"not_found"}),404

# --- REQUISITIONS ---
@app.get("/api/requisitions")
def list_requisitions():
    rows=read_rows("requisitions")
    for r in rows: r["total_amount"]=float(r.get("amount_usd")or 0)
    return jsonify(rows)

@app.get("/api/requisitions/export_csv")
def export_requisitions_csv():
    if require_login(): return require_login()
    rows = read_rows("requisitions")
    si = io.StringIO()
    if rows:
        cw = csv.writer(si)
        keys = list(rows[0].keys())
        cw.writerow(keys)
        for r in rows:
            cw.writerow([r.get(k) for k in keys])
            
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=requisitions.csv"
    output.headers["Content-type"] = "text/csv"
    return output

@app.post("/api/requisitions")
def add_requisition():
    if require_write_access(): return require_write_access()
    d=request.json
    val_paid = 1 if d.get("paid") in [True, "true", 1, "1"] else 0
    # Delivery: 0=No, 1=Full, 2=Partial
    val_delivered = int(d.get("delivered") or 0)
    
    amt_orig = d.get("amount_original")
    if amt_orig is None or amt_orig == "":
        amt_orig = d.get("amount") or 0

    row={
        **d, 
        "id":next_id("requisitions"), 
        "created_by":current_user()["username"], 
        "created_at":now_iso(),
        "paid": val_paid,
        "delivered": val_delivered,
        "amount_original": amt_orig,
        "remarks": d.get("remarks", "")
    }
    row["amount_usd"] = round(to_usd(float(amt_orig), d.get("currency")),2)
    append_row("requisitions", row)
    log_action("Add Req", target=str(row.get("po_number") or row.get("number")), details=row.get("description"))
    return jsonify(row)

@app.patch("/api/requisitions/<int:rid>")
def edit_requisition(rid):
    if require_write_access(): return require_write_access()
    d=request.json
    if "paid" in d: d["paid"] = 1 if d.get("paid") in [True, "true", 1, "1"] else 0
    if "delivered" in d: d["delivered"] = int(d.get("delivered") or 0)
    
    if "amount" in d or "amount_original" in d:
        amt = d.get("amount_original") or d.get("amount") or 0
        d["amount_original"] = amt
        d["amount_usd"] = round(to_usd(float(amt), d.get("currency","USD")),2)
    
    if update_row_by_id("requisitions", rid, d): 
        if "status" in d:
            log_action("Status Change", target=f"Req {rid}", details=d["status"])
        else:
            log_action("Edit Req", target=f"Req {rid}")
        return jsonify({"ok":True})
    return jsonify({"error":"not_found"}),404

@app.delete("/api/requisitions/<int:rid>")
def delete_req(rid):
    if require_admin(): return require_admin()
    delete_row_by_id("requisitions", rid)
    log_action("Delete Req", target=f"Req {rid}")
    return jsonify({"ok":True})

@app.post("/api/requisitions/bulk")
def bulk_req_action():
    if require_write_access(): return require_write_access()
    data = request.json or {}
    ids = data.get("ids", [])
    action = data.get("action", "")
    if not ids or not action: return jsonify({"error":"missing_args"}), 400
    
    updates = {}
    if action == "mark_paid": updates = {"paid": 1}
    elif action == "mark_unpaid": updates = {"paid": 0}
    elif action == "mark_delivered": updates = {"delivered": 1}
    elif action == "mark_undelivered": updates = {"delivered": 0}
    elif action == "mark_partial": updates = {"delivered": 2}
    else: return jsonify({"error":"invalid_action"}), 400
    
    updates["updated_at"] = now_iso()
    count = 0
    for rid in ids:
        if update_row_by_id("requisitions", int(rid), updates):
            count += 1
    
    log_action("Bulk Action", target=action, details=f"Affected {count} orders")
    return jsonify({"ok": True, "updated": count})

# --- LANDINGS ---
@app.get("/api/landings")
def list_landings():
    rows=read_rows("landings")
    for r in rows: r["amount"]=float(r.get("amount_usd")or 0)
    return jsonify(rows)
@app.post("/api/landings")
def add_landing():
    if require_write_access(): return require_write_access()
    d=request.json
    val_paid = 1 if d.get("paid") in [True, "true", 1, "1"] else 0
    val_delivered = int(d.get("delivered") or 0)
    
    amt_orig = d.get("amount_original")
    if amt_orig is None or amt_orig == "":
        amt_orig = d.get("amount") or 0

    row={
        **d, 
        "id":next_id("landings"), 
        "created_by":current_user()["username"], 
        "created_at":now_iso(),
        "paid": val_paid,
        "delivered": val_delivered,
        "amount_original": amt_orig
    }
    row["amount_usd"] = round(to_usd(float(amt_orig), d.get("currency")),2)
    append_row("landings", row)
    log_action("Add Landing", target=row.get("vessel"), details=row.get("item"))
    return jsonify(row)

@app.patch("/api/landings/<int:lid>")
def edit_landing(lid):
    if require_write_access(): return require_write_access()
    d=request.json
    if "paid" in d: d["paid"] = 1 if d.get("paid") in [True, "true", 1, "1"] else 0
    
    if "amount" in d or "amount_original" in d:
        amt = d.get("amount_original") or d.get("amount") or 0
        d["amount_original"] = amt
        d["amount_usd"] = round(to_usd(float(amt), d.get("currency","USD")),2)

    update_row_by_id("landings", lid, d)
    log_action("Edit Landing", target=f"Land {lid}")
    return jsonify({"ok":True})

@app.delete("/api/landings/<int:lid>")
def delete_land(lid):
    if require_admin(): return require_admin()
    delete_row_by_id("landings", lid)
    log_action("Delete Landing", target=f"Land {lid}")
    return jsonify({"ok":True})

@app.post("/api/landings/bulk")
def bulk_land_action():
    if require_write_access(): return require_write_access()
    data = request.json or {}
    ids = data.get("ids", [])
    action = data.get("action", "")
    if not ids or not action: return jsonify({"error":"missing_args"}), 400
    
    updates = {}
    if action == "mark_paid": updates = {"paid": 1}
    elif action == "mark_unpaid": updates = {"paid": 0}
    elif action == "mark_delivered": updates = {"delivered": 1}
    elif action == "mark_undelivered": updates = {"delivered": 0}
    elif action == "mark_partial": updates = {"delivered": 2}
    else: return jsonify({"error":"invalid_action"}), 400
    
    updates["updated_at"] = now_iso()
    count = 0
    for lid in ids:
        if update_row_by_id("landings", int(lid), updates):
            count += 1
            
    log_action("Bulk Land Action", target=action, details=f"Affected {count} items")
    return jsonify({"ok": True, "updated": count})

# --- DIRECTORY ---
@app.get("/api/directory")
def get_dir(): return jsonify(read_rows("directory"))
@app.post("/api/directory")
def add_directory_std():
    if require_write_access(): return require_write_access()
    d=request.json
    row={**d, "id":next_id("directory"), "created_by":current_user()["username"], "created_at":now_iso()}
    append_row("directory", row)
    return jsonify(row)
@app.post("/api/directory/quick")
def add_dir_quick(): return add_directory_std()
@app.patch("/api/directory/<int:did>")
def edit_directory_entry(did: int):
    if require_write_access(): return require_write_access()
    d = request.json or {}
    update_row_by_id("directory", did, d)
    if "rating" in d:
        log_action("Rate Contact", target=f"Dir {did}", details=f"Score: {d.get('rating')}")
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
