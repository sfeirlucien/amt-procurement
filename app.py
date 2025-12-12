"""
AMT Procurement - Robust Flask Backend
UPDATED: Fixed Audit Logs (Timestamp mapping), Duplicate Order support, and Excel robustness.
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
# App Init
# -------------------------------------------------
app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = os.environ.get("SECRET_KEY", "AMT_SECRET_KEY_CHANGE_ME_PLEASE")

# Allow CORS for local development
CORS(app, supports_credentials=True, origins=["*"])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "office_ops.xlsx")
UPLOAD_FOLDER = os.path.join(BASE_DIR, "static", "uploads")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")

# Default Users
DEFAULT_ADMIN = {"username": "admin", "password": "admin123", "role": "admin"}
DEFAULT_FINANCE = {"username": "finance", "password": "finance123", "role": "finance"}

# Schema Definition (Used to validate/repair headers)
SHEET_SCHEMA: Dict[str, List[str]] = {
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
    return datetime.utcnow() + timedelta(hours=4)

def now_iso() -> str:
    return get_dubai_time().isoformat(timespec="seconds")

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

# -------------------------------------------------
# Database (Excel) Logic - ROBUST VERSION
# -------------------------------------------------
def get_wb() -> Workbook:
    """Load workbook or create if missing."""
    if not os.path.exists(DB_FILE):
        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            wb.remove(wb["Sheet"])
        wb.save(DB_FILE)
    return openpyxl.load_workbook(DB_FILE)

def ensure_db() -> None:
    """
    Ensures DB file exists, has all required sheets,
    and has all required columns. Auto-migrates old files.
    """
    if not os.path.exists(DB_FILE):
        wb = Workbook()
        if "Sheet" in wb.sheetnames: wb.remove(wb["Sheet"])
        for sname, headers in SHEET_SCHEMA.items():
            ws = wb.create_sheet(sname)
            ws.append(headers)
        wb.save(DB_FILE)

    wb = openpyxl.load_workbook(DB_FILE)
    modified = False

    # Check Sheets & Columns
    for sname, expected_headers in SHEET_SCHEMA.items():
        if sname not in wb.sheetnames:
            ws = wb.create_sheet(sname)
            ws.append(expected_headers)
            modified = True
        else:
            ws = wb[sname]
            existing_headers = []
            if ws.max_row >= 1:
                existing_headers = [str(cell.value).strip() for cell in ws[1] if cell.value]
            
            for h in expected_headers:
                if h not in existing_headers:
                    ws.cell(row=1, column=ws.max_column + 1).value = h
                    modified = True

    # Ensure Admin Exists
    ws_users = wb["users"]
    users = read_rows("users", wb=wb)
    
    if not any(u.get("username") == "admin" for u in users):
        ws_users.append(["admin", hash_pw(DEFAULT_ADMIN["password"]), "admin", now_iso()])
        modified = True
    
    if not any(u.get("username") == "finance" for u in users):
        ws_users.append(["finance", hash_pw(DEFAULT_FINANCE["password"]), "finance", now_iso()])
        modified = True

    if modified:
        wb.save(DB_FILE)
    
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def read_rows(sheet: str, wb: Workbook = None) -> List[Dict[str, Any]]:
    """Reads rows into list of dicts using ACTUAL file headers."""
    if not wb:
        wb = get_wb()
    if sheet not in wb.sheetnames:
        return []
    
    ws = wb[sheet]
    if ws.max_row < 2:
        return []

    headers = [str(c.value).strip() for c in ws[1]]
    
    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not any(row): continue
        item = {}
        for i, h in enumerate(headers):
            item[h] = row[i] if i < len(row) else None
        out.append(item)
    return out

def append_row(sheet: str, data: Dict[str, Any]) -> None:
    """Appends a row, mapping data dict to actual column positions."""
    wb = get_wb()
    ws = wb[sheet]
    
    headers_map = {}
    for idx, cell in enumerate(ws[1], 1):
        if cell.value:
            headers_map[str(cell.value).strip()] = idx
            
    max_col = ws.max_column
    new_row_vals = [None] * max_col
    
    for key, val in data.items():
        if key in headers_map:
            new_row_vals[headers_map[key] - 1] = val
            
    ws.append(new_row_vals)
    wb.save(DB_FILE)

def update_row_by_id(sheet: str, row_id: int, updates: Dict[str, Any]) -> bool:
    wb = get_wb()
    ws = wb[sheet]
    
    headers_map = {}
    id_col_idx = -1
    for idx, cell in enumerate(ws[1], 1):
        val = str(cell.value).strip() if cell.value else ""
        headers_map[val] = idx
        if val == "id": id_col_idx = idx

    if id_col_idx == -1: return False

    target_row_idx = -1
    for r in range(2, ws.max_row + 1):
        cell_val = ws.cell(r, id_col_idx).value
        if str(cell_val) == str(row_id):
            target_row_idx = r
            break
            
    if target_row_idx == -1: return False

    for k, v in updates.items():
        if k in headers_map:
            ws.cell(target_row_idx, headers_map[k]).value = v
            
    wb.save(DB_FILE)
    return True

def delete_row_by_id(sheet: str, row_id: int) -> bool:
    wb = get_wb()
    ws = wb[sheet]
    
    id_col_idx = -1
    for idx, cell in enumerate(ws[1], 1):
        if str(cell.value).strip() == "id":
            id_col_idx = idx
            break
            
    if id_col_idx == -1: return False

    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, id_col_idx).value) == str(row_id):
            ws.delete_rows(r, 1)
            wb.save(DB_FILE)
            return True
    return False

def next_id(sheet: str) -> int:
    rows = read_rows(sheet)
    if not rows: return 1
    ids = []
    for r in rows:
        try: ids.append(int(r.get("id", 0)))
        except: pass
    return (max(ids) + 1) if ids else 1

# -------------------------------------------------
# Auth & Logging
# -------------------------------------------------
def current_user() -> Optional[Dict[str, str]]:
    if "username" not in session: return None
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
    except: pass

def require_login():
    if not current_user(): return jsonify({"error": "login_required"}), 401

def require_admin():
    u = current_user()
    if not u: return jsonify({"error": "login_required"}), 401
    if u["role"] != "admin": return jsonify({"error": "admin_required"}), 403

def require_write():
    u = current_user()
    if not u: return jsonify({"error": "login_required"}), 401
    if u["role"] == "finance": return jsonify({"error": "read_only"}), 403

# -------------------------------------------------
# FX / Currency
# -------------------------------------------------
def fetch_fx_rates(base="USD"):
    return {"USD": 1.0, "EUR": 0.95, "AED": 3.673, "GBP": 0.79, "SGD": 1.35}

def to_usd(amount, currency):
    try: val = float(amount)
    except: return 0.0
    currency = (currency or "USD").upper()
    if currency == "USD": return val
    rates = fetch_fx_rates()
    rate = rates.get(currency, 1.0)
    return val / rate if rate != 0 else val

# -------------------------------------------------
# Routes
# -------------------------------------------------

@app.before_request
def init_on_first_req():
    if not getattr(app, '_db_checked', False):
        ensure_db()
        app._db_checked = True

@app.route("/")
def home():
    return send_from_directory("static", "index.html")

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "time": now_iso()})

# --- Auth ---
@app.post("/api/login")
def login():
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    users = read_rows("users")
    user = next((u for u in users if u.get("username") == username), None)
    if not user or user.get("password_hash") != hash_pw(password):
        return jsonify({"error": "invalid_credentials"}), 401
    session["username"] = username
    session["role"] = user.get("role", "user")
    log_action("Login")
    return jsonify({"ok": True, "username": username, "role": session["role"]})

@app.post("/api/logout")
def logout():
    session.clear()
    return jsonify({"ok": True})

@app.get("/api/session")
def get_session():
    u = current_user()
    if not u: return jsonify({"logged_in": False}), 401
    return jsonify({"logged_in": True, **u})

# --- Dashboard Core ---
@app.get("/api/currencies")
def get_currencies():
    rates = fetch_fx_rates()
    return jsonify({"currencies": sorted(list(rates.keys()))})

# --- Requisitions ---
@app.get("/api/requisitions")
def list_reqs():
    rows = read_rows("requisitions")
    for r in rows:
        try: r["amount_usd"] = float(r.get("amount_usd") or 0)
        except: r["amount_usd"] = 0.0
    return jsonify(rows)

@app.post("/api/requisitions")
def add_req():
    if require_write(): return require_write()
    d = request.json or {}
    
    amt_origin = d.get("amount_original")
    if amt_origin in [None, ""]: amt_origin = d.get("amount", 0)
    curr = d.get("currency", "USD")
    amt_usd = to_usd(amt_origin, curr)
    
    row = {
        **d,
        "id": next_id("requisitions"),
        "amount_original": amt_origin,
        "amount_usd": round(amt_usd, 2),
        "paid": 1 if d.get("paid") else 0,
        "delivered": int(d.get("delivered", 0)),
        "status": "open",
        "po_number": d.get("po_number", ""),
        "created_at": now_iso(),
        "created_by": current_user()["username"]
    }
    append_row("requisitions", row)
    log_action("Create Req", target=str(row.get("po_number") or row.get("number")))
    return jsonify(row)

@app.patch("/api/requisitions/<int:rid>")
def edit_req(rid):
    if require_write(): return require_write()
    d = request.json or {}
    updates = {**d, "updated_at": now_iso()}
    
    if "amount_original" in d or "amount" in d or "currency" in d:
        amt = d.get("amount_original") or d.get("amount") or 0
        curr = d.get("currency", "USD")
        updates["amount_usd"] = round(to_usd(amt, curr), 2)
        updates["amount_original"] = amt
        
    if "paid" in d: updates["paid"] = 1 if d["paid"] else 0
    
    if update_row_by_id("requisitions", rid, updates):
        log_action("Edit Req", target=str(rid))
        return jsonify({"ok": True})
    return jsonify({"error": "not_found"}), 404

@app.delete("/api/requisitions/<int:rid>")
def del_req(rid):
    if require_admin(): return require_admin()
    delete_row_by_id("requisitions", rid)
    log_action("Delete Req", target=str(rid))
    return jsonify({"ok": True})

@app.post("/api/requisitions/bulk")
def bulk_req():
    if require_write(): return require_write()
    d = request.json or {}
    ids = d.get("ids", [])
    action = d.get("action")
    
    updates = {"updated_at": now_iso()}
    if action == "mark_paid": updates["paid"] = 1
    elif action == "mark_unpaid": updates["paid"] = 0
    elif action == "mark_delivered": updates["delivered"] = 1
    elif action == "mark_partial": updates["delivered"] = 2
    else: return jsonify({"error": "invalid_action"}), 400
    
    count = 0
    for i in ids:
        if update_row_by_id("requisitions", int(i), updates): count += 1
    log_action("Bulk Req", details=f"{action} on {count} items")
    return jsonify({"ok": True, "updated": count})

# --- Landings ---
@app.get("/api/landings")
def list_landings(): return jsonify(read_rows("landings"))

@app.post("/api/landings")
def add_landing():
    if require_write(): return require_write()
    d = request.json or {}
    amt = d.get("amount_original") or d.get("amount") or 0
    curr = d.get("currency", "USD")
    row = {
        **d,
        "id": next_id("landings"),
        "amount_original": amt,
        "amount_usd": round(to_usd(amt, curr), 2),
        "paid": 1 if d.get("paid") else 0,
        "delivered": int(d.get("delivered", 0)),
        "created_at": now_iso(),
        "created_by": current_user()["username"]
    }
    append_row("landings", row)
    log_action("Create Landing", target=row.get("item"))
    return jsonify(row)

@app.patch("/api/landings/<int:lid>")
def edit_landing(lid):
    if require_write(): return require_write()
    d = request.json or {}
    updates = {**d, "updated_at": now_iso()}
    if "amount_original" in d or "amount" in d:
        amt = d.get("amount_original") or d.get("amount") or 0
        curr = d.get("currency", "USD")
        updates["amount_usd"] = round(to_usd(amt, curr), 2)
        updates["amount_original"] = amt
    if "paid" in d: updates["paid"] = 1 if d["paid"] else 0
    update_row_by_id("landings", lid, updates)
    return jsonify({"ok": True})

@app.delete("/api/landings/<int:lid>")
def del_landing(lid):
    if require_admin(): return require_admin()
    delete_row_by_id("landings", lid)
    return jsonify({"ok": True})

@app.post("/api/landings/bulk")
def bulk_land():
    if require_write(): return require_write()
    d = request.json or {}
    ids = d.get("ids", [])
    action = d.get("action")
    updates = {"updated_at": now_iso()}
    if action == "mark_paid": updates["paid"] = 1
    elif action == "mark_unpaid": updates["paid"] = 0
    elif action == "mark_delivered": updates["delivered"] = 1
    elif action == "mark_partial": updates["delivered"] = 2
    else: return jsonify({"error": "invalid_action"}), 400
    for i in ids: update_row_by_id("landings", int(i), updates)
    return jsonify({"ok": True})

# --- Directory ---
@app.get("/api/directory")
def list_dir():
    rows = read_rows("directory")
    rows.sort(key=lambda x: x.get("type", ""))
    return jsonify(rows)

@app.post("/api/directory")
def add_dir():
    if require_write(): return require_write()
    d = request.json or {}
    if not d.get("name"): return jsonify({"error": "name_required"}), 400
    row = {
        **d, "id": next_id("directory"),
        "rating": d.get("rating", 5),
        "created_at": now_iso(),
        "created_by": current_user()["username"]
    }
    append_row("directory", row)
    log_action("Add Contact", target=d.get("name"))
    return jsonify(row)

@app.patch("/api/directory/<int:did>")
def edit_dir(did):
    if require_write(): return require_write()
    d = request.json or {}
    update_row_by_id("directory", did, d)
    return jsonify({"ok": True})

@app.delete("/api/directory/<int:did>")
def del_dir(did):
    if require_admin(): return require_admin()
    delete_row_by_id("directory", did)
    return jsonify({"ok": True})

# --- Admin Resources ---
@app.get("/api/categories")
def get_cats(): return jsonify(read_rows("categories"))

@app.post("/api/categories")
def add_cat():
    if require_admin(): return require_admin()
    d = request.json or {}
    if not d.get("name"): return jsonify({"error": "name_required"}), 400
    row = {"id": next_id("categories"), "name": d["name"], "abbr": d.get("abbr", ""), "created_at": now_iso()}
    append_row("categories", row)
    return jsonify(row)

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
    d = request.json or {}
    if not d.get("name"): return jsonify({"error": "name_required"}), 400
    row = {"id": next_id("vessels"), "name": d["name"], "created_at": now_iso()}
    append_row("vessels", row)
    return jsonify(row)

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
    d = request.json or {}
    if not d.get("username") or not d.get("password"): return jsonify({"error": "fields_required"}), 400
    existing = read_rows("users")
    if any(u["username"] == d["username"] for u in existing): return jsonify({"error": "duplicate_user"}), 409
    row = {"username": d["username"], "password_hash": hash_pw(d["password"]), "role": d.get("role", "user"), "created_at": now_iso()}
    append_row("users", row)
    return jsonify({"ok": True})

@app.delete("/api/users/<username>")
def del_user(username):
    if require_admin(): return require_admin()
    if username == "admin": return jsonify({"error": "cannot_delete_root"}), 400
    wb = get_wb(); ws = wb["users"]
    deleted = False
    u_idx = -1
    for idx, c in enumerate(ws[1], 1):
        if str(c.value).strip() == "username":
            u_idx = idx
            break
    if u_idx != -1:
        for r in range(2, ws.max_row + 1):
            if ws.cell(r, u_idx).value == username:
                ws.delete_rows(r, 1); deleted = True; break
        if deleted: wb.save(DB_FILE)
    return jsonify({"ok": True}) if deleted else (jsonify({"error": "not_found"}), 404)

# --- Logs & Reports ---
@app.get("/api/audit")
def get_logs():
    if require_admin(): return require_admin()
    raw = read_rows("logs")
    # FIX: Map 'timestamp' to 'date' for frontend
    clean = []
    for r in raw:
        clean.append({
            "user": r.get("user"),
            "action": r.get("action"),
            "target": r.get("target"),
            "details": r.get("details"),
            "date": r.get("timestamp")
        })
    clean.reverse()
    return jsonify(clean[:500])

@app.get("/api/reports/aging")
def aging_report():
    if require_login(): return require_login()
    reqs = read_rows("requisitions")
    now = get_dubai_time()
    out = []
    for r in reqs:
        paid = r.get("paid") in [1, True, "1", "true"]
        status = (r.get("status") or "").lower()
        if not paid and status != "cancelled":
            date_str = r.get("date_ordered")
            if not date_str: continue
            try:
                dt = datetime.strptime(str(date_str).split("T")[0], "%Y-%m-%d")
                delta = (now - dt).days
            except: delta = 0
            grp = "< 30 Days"
            if delta > 90: grp = "> 90 Days"
            elif delta > 60: grp = "60-90 Days"
            elif delta > 30: grp = "30-60 Days"
            out.append({
                "po": r.get("po_number") or r.get("number"),
                "supplier": r.get("supplier"),
                "amount": r.get("amount_usd"),
                "days": delta,
                "group": grp
            })
    return jsonify(sorted(out, key=lambda x: x["days"], reverse=True))

# --- Documents ---
@app.post("/api/documents/upload")
def upload_doc():
    if require_write(): return require_write()
    if "file" not in request.files: return jsonify({"error": "no_file"}), 400
    f = request.files["file"]
    if not f.filename: return jsonify({"error": "empty_filename"}), 400
    fname = secure_filename(f.filename)
    save_name = f"{int(time.time())}_{fname}"
    f.save(os.path.join(UPLOAD_FOLDER, save_name))
    row = {
        "id": next_id("documents"),
        "parent_type": request.form.get("parent_type", "req"),
        "parent_id": request.form.get("parent_id", 0),
        "filename": save_name,
        "uploaded_at": now_iso(),
        "uploaded_by": current_user()["username"]
    }
    append_row("documents", row)
    return jsonify({"ok": True})

@app.get("/api/documents/<ptype>/<pid>")
def get_docs(ptype, pid):
    if require_login(): return require_login()
    docs = read_rows("documents")
    matches = [d for d in docs if str(d.get("parent_type")) == str(ptype) and str(d.get("parent_id")) == str(pid)]
    return jsonify(matches)

# --- Backups ---
@app.post("/api/upload")
def upload_restore_db():
    if require_admin(): return require_admin()
    if "file" not in request.files: return jsonify({"error": "no_file"}), 400
    f = request.files["file"]
    tmp_path = os.path.join(BASE_DIR, "temp_restore.xlsx")
    f.save(tmp_path)
    try: openpyxl.load_workbook(tmp_path)
    except: return jsonify({"error": "invalid_excel_file"}), 400
    shutil.copy2(tmp_path, DB_FILE)
    os.remove(tmp_path)
    ensure_db()
    log_action("Restore DB", details="Overwrote DB via upload")
    return jsonify({"ok": True})

@app.post("/api/backup/create")
def create_backup():
    if require_admin(): return require_admin()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"backup_{ts}.xlsx"
    shutil.copy2(DB_FILE, os.path.join(BACKUP_DIR, fname))
    return jsonify({"ok": True})

@app.get("/api/backups")
def list_backups():
    if require_admin(): return require_admin()
    out = []
    if os.path.exists(BACKUP_DIR):
        for f in sorted(os.listdir(BACKUP_DIR), reverse=True):
            if f.endswith(".xlsx"):
                p = os.path.join(BACKUP_DIR, f)
                st = os.stat(p)
                out.append({"name": f, "size": st.st_size, "created_at": datetime.fromtimestamp(st.st_mtime).isoformat()})
    return jsonify(out)

@app.get("/api/backups/<name>/download")
def download_backup(name):
    if require_admin(): return require_admin()
    return send_from_directory(BACKUP_DIR, secure_filename(name), as_attachment=True)

@app.post("/api/backups/<name>/restore")
def restore_backup_file(name):
    if require_admin(): return require_admin()
    src = os.path.join(BACKUP_DIR, secure_filename(name))
    if os.path.exists(src):
        shutil.copy2(src, DB_FILE); ensure_db(); return jsonify({"ok": True})
    return jsonify({"error": "not_found"}), 404

# --- Start ---
if __name__ == "__main__":
    ensure_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
