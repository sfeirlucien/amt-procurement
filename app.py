"""
AMT Procurement - Refactored Flask Backend (Excel DB)

Forensic Audit Corrections:
1. JSON Serialization: Added AMTJSONProvider to handle datetime objects, fixing the "Dashboard Zeros" issue.
2. Type Safety: Implemented string-normalized ID comparison to fix Update/Delete/Toggle failures.
3. Concurrency: Added retry logic with exponential backoff for file saving to mitigate race conditions.
4. Robustness: Added safe_float conversion to prevent crashes on currency formatting.
"""

import os
import json
import hashlib
import shutil
import time
import random
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union

import requests
import openpyxl
from openpyxl import Workbook
from flask import Flask, jsonify, request, session, send_from_directory, send_file
from flask_cors import CORS
from flask.json.provider import DefaultJSONProvider
from werkzeug.utils import secure_filename

# -------------------------------------------------
# App init & Config
# -------------------------------------------------
app = Flask(__name__, static_folder="static", static_url_path="/static")

# SECURITY NOTE: In production, this must be set via environment variable.
app.secret_key = os.environ.get("SECRET_KEY", "AMT_SECRET_KEY_CHANGE_ME")

# Configure Logging to capture file IO errors
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AMT_Procurement")

CORS(app, supports_credentials=True, origins=[
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "https://amt-procurement.onrender.com",
])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# DATA PERSISTENCE WARNING: On Render.com, this file is ephemeral and will be lost on restart.
# Recommended to move to SQLite (with a persistent disk) or PostgreSQL.
DB_FILE = os.path.join(BASE_DIR, "office_ops.xlsx")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")
FX_CACHE_FILE = os.path.join(BASE_DIR, "fx_cache.json")

DEFAULT_ADMIN = {"username": "admin", "password": "admin123", "role": "admin"}
ROLES = {"admin", "user", "viewer", "finance"}
ALLOWED_UPLOAD_EXT = {".xlsx"}
FX_CACHE_TTL_SECONDS = 6 * 60 * 60

# -------------------------------------------------
# FIX 1: Custom JSON Provider for Date Serialization
# Resolves: "Dashboard all showing zeros"
# -------------------------------------------------
class AMTJSONProvider(DefaultJSONProvider):
    def default(self, o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        return super().default(o)

app.json = AMTJSONProvider(app)

# -------------------------------------------------
# Schema Definition
# -------------------------------------------------
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
    # Improved security: simple salt integration could be added here, 
    # but maintaining compatibility with existing hashes first.
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

def safe_float(val: Any) -> float:
    """
    FIX 4: Robust float conversion.
    Resolves: potential 500 errors when users enter currency symbols in Excel.
    """
    if val is None:
        return 0.0
    if isinstance(val, (float, int)):
        return float(val)
    s = str(val).strip().replace("$", "").replace(",", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0

def save_wb_safe(wb: Workbook, filepath: str, retries=5):
    """
    FIX 3: Retry mechanism for file saving.
    Resolves: "Error saving order" due to race conditions or file locks.
    """
    for attempt in range(retries):
        try:
            wb.save(filepath)
            return
        except PermissionError:
            logger.warning(f"File locked, retrying save... (Attempt {attempt+1}/{retries})")
            if attempt < retries - 1:
                # Exponential backoff with jitter
                sleep_time = (0.1 * (2 ** attempt)) + random.uniform(0.01, 0.05)
                time.sleep(sleep_time)
            else:
                logger.error("Failed to save DB after max retries.")
                raise
        except Exception as e:
            logger.error(f"Unexpected save error: {e}")
            raise

def ensure_db() -> None:
    """Ensures DB exists and has correct headers. Handles corrupt columns gracefully."""
    if not os.path.exists(DB_FILE):
        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            wb.remove(wb)
        for sname, headers in SHEETS.items():
            ws = wb.create_sheet(sname)
            ws.append(headers)
        save_wb_safe(wb, DB_FILE)

    wb = openpyxl.load_workbook(DB_FILE)
    modified = False

    # Header synchronization
    for sname, headers in SHEETS.items():
        if sname not in wb.sheetnames:
            ws_new = wb.create_sheet(sname)
            ws_new.append(headers)
            modified = True
            continue
        
        ws_exist = wb[sname]
        if ws_exist.max_row == 0:
            ws_exist.append(headers)
            modified = True
            continue

        # Non-destructive header append
        exist_headers = [str(c.value).strip() for c in ws_exist if c.value]
        for h in headers:
            if h not in exist_headers:
                ws_exist.cell(1, ws_exist.max_column + 1).value = h
                exist_headers.append(h)
                modified = True

    # Ensure admin user
    ws_u = wb["users"]
    u_headers = [str(c.value).strip() for c in ws_u]
    try:
        u_idx = u_headers.index("username") + 1
        p_idx = u_headers.index("password_hash") + 1
        r_idx = u_headers.index("role") + 1
    except ValueError:
        # If headers are missing, we rely on the header sync above to fix it next run
        return 

    admin_found = False
    for r in range(2, ws_u.max_row + 1):
        val = ws_u.cell(r, u_idx).value
        if val and str(val).strip() == "admin":
            admin_found = True
            # Reset default admin password if missing/corrupt
            curr_pw = ws_u.cell(r, p_idx).value
            if not curr_pw or str(curr_pw) == "None":
                ws_u.cell(r, p_idx).value = hash_pw(DEFAULT_ADMIN["password"])
                modified = True
            # Ensure role is admin
            curr_role = ws_u.cell(r, r_idx).value
            if str(curr_role).lower()!= "admin":
                ws_u.cell(r, r_idx).value = "admin"
                modified = True
            break
    
    if not admin_found:
        ws_u.append(), "admin", now_iso()])
        modified = True

    if modified:
        save_wb_safe(wb, DB_FILE)
    
    os.makedirs(BACKUP_DIR, exist_ok=True)

def get_wb() -> Workbook:
    ensure_db()
    return openpyxl.load_workbook(DB_FILE)

def read_rows(sheet: str) -> List]:
    """Reads rows and normalizes data types."""
    wb = get_wb()
    if sheet not in wb.sheetnames:
        return
    ws = wb[sheet]
    # Filter None values from headers
    headers = [str(c.value).strip() for c in ws if c.value]
    
    out: List] =
    # Using values_only=True returns native types (datetime, int, etc.)
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not any(row): continue
        # Zip safely handles if row length < headers length
        r_dict = dict(zip(headers, row))
        
        # Explicit ID typing for frontend consistency
        if "id" in r_dict:
             try: r_dict["id"] = int(safe_float(r_dict["id"]))
             except: pass 
             
        out.append(r_dict)
    return out

def append_row(sheet: str, row: Dict[str, Any]) -> None:
    wb = get_wb()
    ws = wb[sheet]
    headers = [str(c.value).strip() for c in ws if c.value]
    
    # Map dictionary to list based on current headers
    row_values =
    for h in headers:
        row_values.append(row.get(h))
        
    ws.append(row_values)
    save_wb_safe(wb, DB_FILE)

def next_id(sheet: str) -> int:
    rows = read_rows(sheet)
    mx = 0
    for r in rows:
        try:
            val = int(safe_float(r.get("id")))
            if val > mx: mx = val
        except Exception:
            pass
    return mx + 1

def update_row_by_id(sheet: str, row_id: int, updates: Dict[str, Any]) -> bool:
    """
    FIX 2: Type-agnostic ID comparison.
    Resolves: "Error toggling paid/delivered" and "Error cancelling".
    """
    wb = get_wb()
    ws = wb[sheet]
    
    # Locate ID column dynamically
    headers = [str(c.value).strip() for c in ws]
    if "id" not in headers:
        return False
    id_col = headers.index("id") + 1
    
    target_str = str(row_id).strip()
    match_row = None
    
    for r_idx in range(2, ws.max_row + 1):
        cell_val = ws.cell(r_idx, id_col).value
        # Compare as strings to handle mixed types in Excel (int vs string)
        if str(cell_val).strip() == target_str:
            match_row = r_idx
            break
            
    if not match_row:
        return False
        
    for k, v in updates.items():
        if k in headers:
            c_idx = headers.index(k) + 1
            ws.cell(match_row, c_idx).value = v
            
    save_wb_safe(wb, DB_FILE)
    return True

def delete_row_by_id(sheet: str, row_id: int) -> bool:
    """
    FIX 2: Type-agnostic ID comparison.
    Resolves: "Error deleting".
    """
    wb = get_wb()
    ws = wb[sheet]
    
    headers = [str(c.value).strip() for c in ws]
    if "id" not in headers:
        return False
    id_col = headers.index("id") + 1
    
    target_str = str(row_id).strip()
    
    for r_idx in range(2, ws.max_row + 1):
        cell_val = ws.cell(r_idx, id_col).value
        if str(cell_val).strip() == target_str:
            ws.delete_rows(r_idx, 1)
            save_wb_safe(wb, DB_FILE)
            return True
    return False

def log_action(action: str, details: str = "") -> None:
    try:
        append_row("logs", {"timestamp": now_iso(), "action": action, "details": details})
    except:
        pass # Logging should not break app flow

# -------------------------------------------------
# Auth Helpers
# -------------------------------------------------
def current_user() -> Optional]:
    if "username" not in session: return None
    return {"username": session["username"], "role": session.get("role", "user")}

def require_login():
    if not current_user(): return jsonify({"error": "login_required"}), 401

def require_admin():
    u = current_user()
    if not u: return jsonify({"error": "login_required"}), 401
    if u["role"]!= "admin": return jsonify({"error": "admin_required"}), 403

def can_edit_row(row: Dict[str, Any]) -> bool:
    u = current_user()
    if not u: return False
    if u["role"] == "admin": return True
    return (row.get("created_by") or "") == u["username"]

# -------------------------------------------------
# FX Helpers
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
    if cache.get("timestamp") and cache.get("base") == base:
        if (datetime.utcnow().timestamp() - float(cache["timestamp"])) < FX_CACHE_TTL_SECONDS:
            return cache.get("rates", {})
            
    # Fallback default rates
    defaults = {"USD": 1.0, "EUR": 0.9, "GBP": 0.78, "AED": 3.67}
    
    try:
        r = requests.get(
            "https://api.exchangerate.host/latest", 
            params={"base": base}, 
            timeout=5
        )
        if r.ok:
            data = r.json()
            rates = data.get("rates", {})
            if rates:
                save_fx_cache({"timestamp": datetime.utcnow().timestamp(), "base": base, "rates": rates})
                return rates
    except:
        pass
        
    return cache.get("rates", defaults)

def to_usd(amount: float, currency: str) -> float:
    try:
        amount = float(amount)
    except:
        return 0.0
        
    currency = (currency or "USD").upper()
    if currency == "USD": return amount
    rates = fetch_fx_rates("USD")
    rate = rates.get(currency)
    if not rate: return amount
    return amount / rate

# -------------------------------------------------
# Routes
# -------------------------------------------------
@app.get("/")
def home():
    return send_from_directory("static", "index.html")

@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "time": now_iso()})

# -- Auth --
@app.post("/api/login")
def login():
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    users = read_rows("users")
    u = next((x for x in users if str(x.get("username")) == username), None)
    
    if not u or u.get("password_hash")!= hash_pw(password):
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

# -- FX --
@app.get("/api/currencies")
def api_currencies():
    rates = fetch_fx_rates("USD")
    return jsonify({"currencies": sorted(set(rates.keys()) | {"USD"})})

# -- Requisitions --
@app.get("/api/requisitions")
def list_requisitions():
    rows = read_rows("requisitions")
    # Fix aliases for frontend compatibility
    for r in rows:
        r["total_amount"] = r.get("amount_usd")
        r["original_amount"] = r.get("amount_original")
    return jsonify(rows)

@app.post("/api/requisitions")
def add_requisition():
    if err := require_login(): return err
    data = request.json or {}
    
    # Input extraction & Sanitization
    number = str(data.get("number") or "").strip()
    vessel = str(data.get("vessel") or "").strip()
    amount_raw = data.get("amount") or data.get("total_amount")
    
    if not number or not vessel:
        return jsonify({"error": "missing_fields"}), 400
        
    amount = safe_float(amount_raw)
    if amount <= 0:
        return jsonify({"error": "invalid_amount"}), 400
        
    currency = (data.get("currency") or "USD").upper()
    
    # Duplicate check (Robust string comparison)
    reqs = read_rows("requisitions")
    if any(str(r.get("number")) == number for r in reqs):
        return jsonify({"error": "duplicate_number"}), 409
        
    row = {
        "id": next_id("requisitions"),
        "number": number,
        "description": (data.get("description") or "").strip(),
        "vessel": vessel,
        "category": (data.get("category") or "").strip().upper(),
        "supplier": (data.get("supplier") or "").strip(),
        "date_ordered": data.get("date_ordered"),
        "expected": data.get("expected"),
        "amount_original": amount,
        "currency": currency,
        "amount_usd": round(to_usd(amount, currency), 2),
        "paid": 1 if data.get("paid") else 0,
        "delivered": 1 if data.get("delivered") else 0,
        "status": (data.get("status") or "open").lower(),
        "po_number": (data.get("po_number") or "").strip(),
        "remarks": (data.get("remarks") or "").strip(),
        "urgency": (data.get("urgency") or "normal").lower(),
        "created_by": current_user()["username"],
        "created_at": now_iso(),
        "updated_at": now_iso()
    }
    append_row("requisitions", row)
    return jsonify(row)

@app.patch("/api/requisitions/<int:rid>")
def edit_requisition(rid: int):
    if err := require_login(): return err
    
    reqs = read_rows("requisitions")
    # Fuzzy match ID
    row = next((r for r in reqs if int(safe_float(r.get("id"))) == rid), None)
    if not row: return jsonify({"error": "not_found"}), 404
    
    if not can_edit_row(row): return jsonify({"error": "not_allowed"}), 403
    
    data = request.json or {}
    updates = {}
    
    # Simple fields
    for k in ["number", "description", "vessel", "supplier", "date_ordered", "expected", "po_number", "remarks", "urgency", "status"]:
        if k in data: updates[k] = str(data[k]).strip()
    
    if "category" in data: updates["category"] = str(data["category"]).strip().upper()
    if "paid" in data: updates["paid"] = 1 if data["paid"] else 0
    if "delivered" in data: updates["delivered"] = 1 if data["delivered"] else 0
    
    # Recalculate amounts if needed
    recalc = False
    new_amt = row.get("amount_original")
    new_curr = row.get("currency")
    
    if "amount" in data or "total_amount" in data:
        new_amt = safe_float(data.get("amount") or data.get("total_amount"))
        updates["amount_original"] = new_amt
        recalc = True
        
    if "currency" in data:
        new_curr = str(data["currency"]).upper()
        updates["currency"] = new_curr
        recalc = True
        
    if recalc:
        updates["amount_usd"] = round(to_usd(new_amt, new_curr), 2)
        
    updates["updated_at"] = now_iso()
    
    if update_row_by_id("requisitions", rid, updates):
        return jsonify({"ok": True})
    return jsonify({"error": "update_failed"}), 500

@app.patch("/api/requisitions/<int:rid>/toggle_paid")
def toggle_paid_req(rid: int):
    if err := require_login(): return err
    reqs = read_rows("requisitions")
    row = next((r for r in reqs if int(safe_float(r.get("id"))) == rid), None)
    if not row: return jsonify({"error": "not_found"}), 404
    if not can_edit_row(row): return jsonify({"error": "not_allowed"}), 403
    
    cur_val = int(safe_float(row.get("paid")))
    update_row_by_id("requisitions", rid, {"paid": 0 if cur_val else 1, "updated_at": now_iso()})
    return jsonify({"ok": True})

@app.delete("/api/requisitions/<int:rid>")
def delete_requisition(rid: int):
    if err := require_admin(): return err
    if delete_row_by_id("requisitions", rid):
        return jsonify({"ok": True})
    return jsonify({"error": "not_found"}), 404

# -- Landings --
@app.get("/api/landings")
def list_landings():
    rows = read_rows("landings")
    for r in rows:
        r["amount"] = r.get("amount_usd")
        r["original_amount"] = r.get("amount_original")
    return jsonify(rows)

@app.post("/api/landings")
def add_landing():
    if err := require_login(): return err
    data = request.json or {}
    
    vessel = str(data.get("vessel") or "").strip()
    item = str(data.get("item") or data.get("description") or "").strip()
    amount = safe_float(data.get("amount"))
    
    if not vessel or not item or amount <= 0:
        return jsonify({"error": "missing_fields"}), 400
        
    currency = (data.get("currency") or "USD").upper()
    
    row = {
        "id": next_id("landings"),
        "vessel": vessel,
        "item": item,
        "workshop": (data.get("workshop") or "").strip(),
        "expected": data.get("expected"),
        "landed_date": data.get("landed_date"),
        "amount_original": amount,
        "currency": currency,
        "amount_usd": round(to_usd(amount, currency), 2),
        "paid": 1 if data.get("paid") else 0,
        "delivered": 1 if data.get("delivered") else 0,
        "status": (data.get("status") or "open").lower(),
        "created_by": current_user()["username"],
        "created_at": now_iso(),
        "updated_at": now_iso()
    }
    append_row("landings", row)
    return jsonify(row)

@app.patch("/api/landings/<int:lid>")
def edit_landing(lid: int):
    if err := require_login(): return err
    lands = read_rows("landings")
    row = next((r for r in lands if int(safe_float(r.get("id"))) == lid), None)
    if not row: return jsonify({"error": "not_found"}), 404
    if not can_edit_row(row): return jsonify({"error": "not_allowed"}), 403
    
    data = request.json or {}
    updates = {}
    
    for k in ["vessel", "item", "workshop", "expected", "landed_date", "status"]:
        if k in data: updates[k] = str(data[k]).strip()
        
    if "paid" in data: updates["paid"] = 1 if data["paid"] else 0
    if "delivered" in data: updates["delivered"] = 1 if data["delivered"] else 0
    
    recalc = False
    new_amt = row.get("amount_original")
    new_curr = row.get("currency")
    
    if "amount" in data:
        new_amt = safe_float(data["amount"])
        updates["amount_original"] = new_amt
        recalc = True
    if "currency" in data:
        new_curr = str(data["currency"]).upper()
        updates["currency"] = new_curr
        recalc = True
        
    if recalc:
        updates["amount_usd"] = round(to_usd(new_amt, new_curr), 2)
        
    updates["updated_at"] = now_iso()
    if update_row_by_id("landings", lid, updates):
        return jsonify({"ok": True})
    return jsonify({"error": "update_failed"}), 500

@app.patch("/api/landings/<int:lid>/toggle_paid")
def toggle_paid_land(lid: int):
    if err := require_login(): return err
    lands = read_rows("landings")
    row = next((r for r in lands if int(safe_float(r.get("id"))) == lid), None)
    if not row: return jsonify({"error": "not_found"}), 404
    if not can_edit_row(row): return jsonify({"error": "not_allowed"}), 403
    
    cur_val = int(safe_float(row.get("paid")))
    update_row_by_id("landings", lid, {"paid": 0 if cur_val else 1, "updated_at": now_iso()})
    return jsonify({"ok": True})

@app.delete("/api/landings/<int:lid>")
def delete_landing(lid: int):
    if err := require_admin(): return err
    if delete_row_by_id("landings", lid):
        return jsonify({"ok": True})
    return jsonify({"error": "not_found"}), 404

# -- Directory (Basic CRUD) --
@app.get("/api/directory")
def list_directory():
    dtype = request.args.get("type")
    rows = read_rows("directory")
    if dtype:
        rows = [r for r in rows if str(r.get("type")).lower() == dtype.lower()]
    return jsonify(rows)

@app.post("/api/directory")
def add_directory():
    if err := require_login(): return err
    data = request.json or {}
    row = {
        "id": next_id("directory"),
        "type": (data.get("type") or "").strip().lower(),
        "name": (data.get("name") or "").strip(),
        "email": (data.get("email") or "").strip(),
        "phone": (data.get("phone") or "").strip(),
        "address": (data.get("address") or "").strip(),
        "created_by": current_user()["username"],
        "created_at": now_iso()
    }
    if not row["name"]: return jsonify({"error": "missing_name"}), 400
    append_row("directory", row)
    return jsonify(row)

@app.post("/api/directory/quick")
def add_directory_quick():
    return add_directory()

# -- Admin (Categories, Vessels, Users, Backups) --
@app.get("/api/categories")
def get_categories():
    return jsonify(read_rows("categories"))

@app.post("/api/categories")
def add_category():
    if err := require_admin(): return err
    data = request.json or {}
    row = {
        "id": next_id("categories"),
        "name": (data.get("name") or "").strip(),
        "abbr": (data.get("abbr") or "").strip().upper(),
        "created_at": now_iso()
    }
    if not row["name"] or not row["abbr"]: return jsonify({"error": "missing_fields"}), 400
    
    cats = read_rows("categories")
    if any(str(c.get("abbr")) == row["abbr"] for c in cats):
        return jsonify({"error": "duplicate_abbr"}), 409
        
    append_row("categories", row)
    return jsonify(row)

@app.delete("/api/categories/<int:cid>")
def delete_category(cid: int):
    if err := require_admin(): return err
    delete_row_by_id("categories", cid)
    return jsonify({"ok": True})

@app.get("/api/vessels")
def get_vessels():
    return jsonify(read_rows("vessels"))

@app.post("/api/vessels")
def add_vessel():
    if err := require_admin(): return err
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name: return jsonify({"error": "missing_name"}), 400
    
    row = {"id": next_id("vessels"), "name": name, "created_at": now_iso()}
    append_row("vessels", row)
    return jsonify(row)

@app.delete("/api/vessels/<int:vid>")
def delete_vessel(vid: int):
    if err := require_admin(): return err
    delete_row_by_id("vessels", vid)
    return jsonify({"ok": True})

@app.get("/api/users")
def get_users():
    if err := require_admin(): return err
    return jsonify(read_rows("users"))

@app.post("/api/users")
def add_user():
    if err := require_admin(): return err
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password: return jsonify({"error": "missing_fields"}), 400
    
    users = read_rows("users")
    if any(str(u.get("username")) == username for u in users):
        return jsonify({"error": "duplicate"}), 409
        
    append_row("users", {
        "username": username,
        "password_hash": hash_pw(password),
        "role": (data.get("role") or "user").strip().lower(),
        "created_at": now_iso()
    })
    return jsonify({"ok": True})

@app.delete("/api/users/<username>")
def delete_user(username: str):
    if err := require_admin(): return err
    if username == "admin": return jsonify({"error": "cannot_delete_root"}), 400
    
    wb = get_wb()
    ws = wb["users"]
    u_col = 1 # fallback
    headers = [str(c.value) for c in ws]
    if "username" in headers: u_col = headers.index("username") + 1
    
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, u_col).value) == username:
            ws.delete_rows(r, 1)
            save_wb_safe(wb, DB_FILE)
            return jsonify({"ok": True})
            
    return jsonify({"error": "not_found"}), 404

# -- Backups --
@app.get("/api/backups")
def list_backups():
    if err := require_admin(): return err
    ensure_db()
    files =
    for f in os.listdir(BACKUP_DIR):
        if f.endswith(".xlsx"):
            path = os.path.join(BACKUP_DIR, f)
            st = os.stat(path)
            files.append({
                "name": f,
                "size": st.st_size,
                "created_at": datetime.utcfromtimestamp(st.st_mtime).isoformat()
            })
    return jsonify(sorted(files, key=lambda x: x["name"], reverse=True))

@app.get("/api/backup")
def create_backup():
    if err := require_admin(): return err
    ensure_db()
    name = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join(BACKUP_DIR, name)
    shutil.copy2(DB_FILE, path)
    return send_file(path, as_attachment=True, download_name=name)

# -------------------------------------------------
# Main
# -------------------------------------------------
if __name__ == "__main__":
    ensure_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
