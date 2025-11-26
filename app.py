import os
import json
import hashlib
import shutil
import time
import random
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import requests
import openpyxl
from openpyxl import Workbook
from flask import Flask, jsonify, request, session, send_from_directory, send_file
from flask_cors import CORS
from flask.json.provider import DefaultJSONProvider
from werkzeug.utils import secure_filename

# ============================================================
#  APP INITIALIZATION
# ============================================================

app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = os.environ.get("SECRET_KEY", "AMT_SECRET_KEY_CHANGE_ME")

CORS(app, supports_credentials=True, origins=[
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "https://amt-procurement.onrender.com",
])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "office_ops.xlsx")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")
FX_CACHE_FILE = os.path.join(BASE_DIR, "fx_cache.json")

DEFAULT_ADMIN = {"username": "admin", "password": "admin123", "role": "admin"}
ALLOWED_UPLOAD_EXT = {".xlsx"}
ROLES = {"admin", "user", "viewer", "finance"}
FX_CACHE_TTL_SECONDS = 6 * 60 * 60

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AMT_Procurement")

# ============================================================
#  JSON PROVIDER (FIXES DASHBOARD ZERO PROBLEM)
# ============================================================

class AMTJSONProvider(DefaultJSONProvider):
    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)

app.json = AMTJSONProvider(app)

# ============================================================
#  EXCEL SCHEMA
# ============================================================

SHEETS = {
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

# ============================================================
#  HELPER FUNCTIONS
# ============================================================

def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def safe_float(val: Any) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace(",", "").replace("$", "").strip()
    try:
        return float(s)
    except:
        return 0.0

def save_wb_safe(wb: Workbook, path: str, retries=6):
    """Prevents Excel saving corruption."""
    for i in range(retries):
        try:
            wb.save(path)
            return
        except PermissionError:
            time.sleep(0.05 + (i * 0.1))
        except Exception as e:
            raise
    raise Exception("Could not save Excel file")

def ensure_db():
    """Creates DB with correct headers. Ensures admin exists."""
    if not os.path.exists(DB_FILE):
        wb = Workbook()
        for name, headers in SHEETS.items():
            ws = wb.create_sheet(name)
            ws.append(headers)
        if "Sheet" in wb.sheetnames:
            del wb["Sheet"]
        save_wb_safe(wb, DB_FILE)

    wb = openpyxl.load_workbook(DB_FILE)
    modified = False

    # Ensure sheets & headers
    for sname, headers in SHEETS.items():
        if sname not in wb.sheetnames:
            ws = wb.create_sheet(sname)
            ws.append(headers)
            modified = True
        else:
            ws = wb[sname]
            existing = [str(c.value) for c in ws[1] if c.value]
            for h in headers:
                if h not in existing:
                    ws.cell(1, ws.max_column + 1).value = h
                    modified = True

    # Ensure admin exists
    ws_u = wb["users"]
    usernames = [ws_u.cell(r, 1).value for r in range(2, ws_u.max_row + 1)]
    if "admin" not in usernames:
        ws_u.append([
            DEFAULT_ADMIN["username"],
            hash_pw(DEFAULT_ADMIN["password"]),
            "admin",
            now_iso()
        ])
        modified = True

    if modified:
        save_wb_safe(wb, DB_FILE)

    os.makedirs(BACKUP_DIR, exist_ok=True)

def get_wb():
    ensure_db()
    return openpyxl.load_workbook(DB_FILE)

def read_rows(sheet: str) -> List[Dict[str, Any]]:
    """Reads Excel sheet into list of dictionaries."""
    wb = get_wb()
    ws = wb[sheet]
    headers = [str(c.value) for c in ws[1]]

    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not any(row):
            continue
        d = dict(zip(headers, row))
        if "id" in d and d["id"] is not None:
            try:
                d["id"] = int(safe_float(d["id"]))
            except:
                pass
        rows.append(d)
    return rows

def append_row(sheet: str, data: dict):
    wb = get_wb()
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    row = [data.get(h) for h in headers]
    ws.append(row)
    save_wb_safe(wb, DB_FILE)

def next_id(sheet: str) -> int:
    rows = read_rows(sheet)
    ids = [int(safe_float(r.get("id"))) for r in rows if r.get("id")]
    return max(ids, default=0) + 1

def update_row_by_id(sheet: str, row_id: int, updates: dict) -> bool:
    wb = get_wb()
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]

    id_col = headers.index("id") + 1
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, id_col).value).strip() == str(row_id):
            for k, v in updates.items():
                if k in headers:
                    c = headers.index(k) + 1
                    ws.cell(r, c).value = v
            save_wb_safe(wb, DB_FILE)
            return True
    return False

def delete_row_by_id(sheet: str, row_id: int) -> bool:
    wb = get_wb()
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    id_col = headers.index("id") + 1

    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, id_col).value).strip() == str(row_id):
            ws.delete_rows(r)
            save_wb_safe(wb, DB_FILE)
            return True
    return False

# ============================================================
#  LOGGING
# ============================================================

def log_action(action: str, details: str = ""):
    try:
        append_row("logs", {
            "timestamp": now_iso(),
            "action": action,
            "details": details
        })
    except:
        pass  # logging must never break the app

# ============================================================
#  AUTHENTICATION HELPERS
# ============================================================

def current_user() -> Optional[Dict[str, Any]]:
    if "username" not in session:
        return None
    return {
        "username": session["username"],
        "role": session.get("role", "user")
    }

def require_login():
    if not current_user():
        return jsonify({"error": "login_required"}), 401

def require_admin():
    u = current_user()
    if not u:
        return jsonify({"error": "login_required"}), 401
    if u["role"] != "admin":
        return jsonify({"error": "admin_required"}), 403

def can_edit_row(row: Dict[str, Any]) -> bool:
    u = current_user()
    if not u:
        return False
    if u["role"] == "admin":
        return True
    return (row.get("created_by") or "") == u["username"]

# ============================================================
#  FX (CURRENCY) HELPERS
# ============================================================

def load_fx_cache():
    if not os.path.exists(FX_CACHE_FILE):
        return {}
    try:
        with open(FX_CACHE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_fx_cache(cache: dict):
    try:
        with open(FX_CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except:
        pass

def fetch_fx_rates(base="USD") -> Dict[str, float]:
    cache = load_fx_cache()

    # Valid cache?
    if (
        cache.get("timestamp")
        and cache.get("base") == base
        and (datetime.utcnow().timestamp() - cache["timestamp"]) < FX_CACHE_TTL_SECONDS
    ):
        return cache.get("rates", {})

    # fallback defaults
    fallback = {"USD": 1.0, "EUR": 0.9, "GBP": 0.78, "AED": 3.67, "LBP": 89500}

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
                save_fx_cache({
                    "timestamp": datetime.utcnow().timestamp(),
                    "base": base,
                    "rates": rates
                })
                return rates
    except:
        pass

    return cache.get("rates", fallback)

def to_usd(amount: float, currency: str) -> float:
    currency = (currency or "USD").upper()
    if currency == "USD":
        return amount

    rates = fetch_fx_rates("USD")
    rate = rates.get(currency)
    if not rate:
        return amount  # fallback

    try:
        return float(amount) / float(rate)
    except:
        return amount

# ============================================================
#  ROUTES: CORE ASSETS
# ============================================================

@app.get("/")
def home():
    return send_from_directory("static", "index.html")

@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "time": now_iso()})

# ============================================================
#  AUTH ROUTES
# ============================================================

@app.post("/api/login")
def login():
    data = request.json or {}
    username = str(data.get("username") or "").strip()
    password = data.get("password") or ""

    users = read_rows("users")
    row = next((u for u in users if u.get("username") == username), None)

    if not row or row.get("password_hash") != hash_pw(password):
        return jsonify({"error": "invalid_credentials"}), 401

    session["username"] = username
    session["role"] = row.get("role", "user")
    log_action("login", username)

    return jsonify({
        "ok": True,
        "username": username,
        "role": session["role"]
    })

@app.post("/api/logout")
def logout():
    session.clear()
    return jsonify({"ok": True})

@app.get("/api/session")
def session_info():
    u = current_user()
    if not u:
        return jsonify({"logged_in": False}), 401
    return jsonify({"logged_in": True, **u})

# ============================================================
#  FX ROUTES
# ============================================================

@app.get("/api/currencies")
def api_currencies():
    rates = fetch_fx_rates("USD")
    currencies = sorted(set(rates.keys()) | {"USD"})
    return jsonify({"currencies": currencies})

# ============================================================
#  REQUISITIONS — LIST
# ============================================================

@app.get("/api/requisitions")
def list_requisitions():
    rows = read_rows("requisitions")

    # adapt fields for frontend
    for r in rows:
        r["total_amount"] = r.get("amount_usd")
        r["original_amount"] = r.get("amount_original")

    return jsonify(rows)

# ============================================================
#  REQUISITIONS — ADD
# ============================================================

@app.post("/api/requisitions")
def add_requisition():
    if err := require_login():
        return err

    data = request.json or {}

    number = str(data.get("number") or "").strip()
    vessel = str(data.get("vessel") or "").strip()
    supplier = str(data.get("supplier") or "").strip()
    amount = safe_float(data.get("amount") or data.get("total_amount"))

    if not number or not vessel or not supplier:
        return jsonify({"error": "missing_fields"}), 400

    if amount <= 0:
        return jsonify({"error": "invalid_amount"}), 400

    # duplicate check
    rows = read_rows("requisitions")
    if any(str(r.get("number")) == number for r in rows):
        return jsonify({"error": "duplicate_number"}), 409

    currency = (data.get("currency") or "USD").upper()

    new_row = {
        "id": next_id("requisitions"),
        "number": number,
        "description": (data.get("description") or "").strip(),
        "vessel": vessel,
        "category": (data.get("category") or "").upper(),
        "supplier": supplier,
        "date_ordered": data.get("date_ordered"),
        "expected": data.get("expected"),
        "amount_original": amount,
        "currency": currency,
        "amount_usd": round(to_usd(amount, currency), 2),
        "paid": 1 if data.get("paid") else 0,
        "delivered": 0,
        "status": "open",
        "po_number": (data.get("po_number") or "").strip(),
        "remarks": (data.get("remarks") or "").strip(),
        "urgency": (data.get("urgency") or "normal").lower(),
        "created_by": current_user()["username"],
        "created_at": now_iso(),
        "updated_at": now_iso()
    }

    append_row("requisitions", new_row)
    return jsonify(new_row)

# ============================================================
#  REQUISITIONS — EDIT
# ============================================================

@app.patch("/api/requisitions/<int:rid>")
def edit_requisition(rid):
    if err := require_login():
        return err

    rows = read_rows("requisitions")
    row = next((r for r in rows if r.get("id") == rid), None)

    if not row:
        return jsonify({"error": "not_found"}), 404

    if not can_edit_row(row):
        return jsonify({"error": "not_allowed"}), 403

    data = request.json or {}
    updates = {}

    for k in ["number", "description", "vessel", "supplier", "date_ordered",
              "expected", "po_number", "remarks", "urgency", "status"]:
        if k in data:
            updates[k] = str(data[k]).strip()

    if "category" in data:
        updates["category"] = str(data["category"]).upper()

    if "paid" in data:
        updates["paid"] = 1 if data["paid"] else 0

    if "delivered" in data:
        updates["delivered"] = 1 if data["delivered"] else 0

    # amount recalc
    recalc = False
    amount = row.get("amount_original")
    currency = row.get("currency")

    if "amount" in data or "total_amount" in data:
        amount = safe_float(data.get("amount") or data.get("total_amount"))
        updates["amount_original"] = amount
        recalc = True

    if "currency" in data:
        currency = data["currency"].upper()
        updates["currency"] = currency
        recalc = True

    if recalc:
        updates["amount_usd"] = round(to_usd(amount, currency), 2)

    updates["updated_at"] = now_iso()

    ok = update_row_by_id("requisitions", rid, updates)
    if not ok:
        return jsonify({"error": "update_failed"}), 500

    return jsonify({"ok": True})

# ============================================================
#  REQUISITIONS — TOGGLE PAID
# ============================================================

@app.patch("/api/requisitions/<int:rid>/toggle_paid")
def toggle_paid_requisition(rid):
    if err := require_login():
        return err

    rows = read_rows("requisitions")
    row = next((r for r in rows if r.get("id") == rid), None)

    if not row:
        return jsonify({"error": "not_found"}), 404

    if not can_edit_row(row):
        return jsonify({"error": "not_allowed"}), 403

    new_val = 0 if row.get("paid") else 1

    update_row_by_id("requisitions", rid, {
        "paid": new_val,
        "updated_at": now_iso()
    })

    return jsonify({"ok": True})


# ============================================================
#  REQUISITIONS — DELETE
# ============================================================

@app.delete("/api/requisitions/<int:rid>")
def delete_requisition(rid):
    if err := require_admin():
        return err

    if delete_row_by_id("requisitions", rid):
        return jsonify({"ok": True})

    return jsonify({"error": "not_found"}), 404


# ============================================================
#  LANDINGS — LIST
# ============================================================

@app.get("/api/landings")
def list_landings():
    rows = read_rows("landings")
    for r in rows:
        r["amount"] = r.get("amount_usd")
        r["original_amount"] = r.get("amount_original")
    return jsonify(rows)


# ============================================================
#  LANDINGS — ADD
# ============================================================

@app.post("/api/landings")
def add_landing():
    if err := require_login():
        return err

    data = request.json or {}

    vessel = str(data.get("vessel") or "").strip()
    item = str(data.get("item") or data.get("description") or "").strip()
    workshop = str(data.get("workshop") or "").strip()
    amount = safe_float(data.get("amount"))

    if not vessel or not item or not workshop:
        return jsonify({"error": "missing_fields"}), 400

    if amount <= 0:
        return jsonify({"error": "invalid_amount"}), 400

    currency = (data.get("currency") or "USD").upper()

    new_row = {
        "id": next_id("landings"),
        "vessel": vessel,
        "item": item,
        "workshop": workshop,
        "expected": data.get("expected"),
        "landed_date": data.get("landed_date"),
        "amount_original": amount,
        "currency": currency,
        "amount_usd": round(to_usd(amount, currency), 2),
        "paid": 1 if data.get("paid") else 0,
        "delivered": 0,
        "status": "open",
        "created_by": current_user()["username"],
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }

    append_row("landings", new_row)
    return jsonify(new_row)


# ============================================================
#  LANDINGS — EDIT
# ============================================================

@app.patch("/api/landings/<int:lid>")
def edit_landing(lid):
    if err := require_login():
        return err

    rows = read_rows("landings")
    row = next((r for r in rows if r.get("id") == lid), None)

    if not row:
        return jsonify({"error": "not_found"}), 404

    if not can_edit_row(row):
        return jsonify({"error": "not_allowed"}), 403

    data = request.json or {}
    updates = {}

    for k in ["vessel", "item", "workshop", "expected", "landed_date", "status"]:
        if k in data:
            updates[k] = str(data[k]).strip()

    if "paid" in data:
        updates["paid"] = 1 if data["paid"] else 0

    if "delivered" in data:
        updates["delivered"] = 1 if data["delivered"] else 0

    # recalc
    recalc = False
    amount = row.get("amount_original")
    currency = row.get("currency")

    if "amount" in data:
        amount = safe_float(data["amount"])
        updates["amount_original"] = amount
        recalc = True

    if "currency" in data:
        currency = data["currency"].upper()
        updates["currency"] = currency
        recalc = True

    if recalc:
        updates["amount_usd"] = round(to_usd(amount, currency), 2)

    updates["updated_at"] = now_iso()

    ok = update_row_by_id("landings", lid, updates)
    if not ok:
        return jsonify({"error": "update_failed"}), 500

    return jsonify({"ok": True})


# ============================================================
#  LANDINGS — TOGGLE PAID
# ============================================================

@app.patch("/api/landings/<int:lid>/toggle_paid")
def toggle_paid_landing(lid):
    if err := require_login():
        return err

    rows = read_rows("landings")
    row = next((r for r in rows if r.get("id") == lid), None)

    if not row:
        return jsonify({"error": "not_found"}), 404

    if not can_edit_row(row):
        return jsonify({"error": "not_allowed"}), 403

    new_val = 0 if row.get("paid") else 1

    update_row_by_id("landings", lid, {
        "paid": new_val,
        "updated_at": now_iso()
    })

    return jsonify({"ok": True})


# ============================================================
#  LANDINGS — DELETE
# ============================================================

@app.delete("/api/landings/<int:lid>")
def delete_landing(lid):
    if err := require_admin():
        return err

    if delete_row_by_id("landings", lid):
        return jsonify({"ok": True})

    return jsonify({"error": "not_found"}), 404


# ============================================================
#  DIRECTORY — LIST / FILTER
# ============================================================

@app.get("/api/directory")
def directory_list():
    dtype = request.args.get("type")
    rows = read_rows("directory")
    if dtype:
        rows = [r for r in rows if str(r.get("type")).lower() == dtype.lower()]
    return jsonify(rows)


# ============================================================
#  DIRECTORY — ADD
# ============================================================

@app.post("/api/directory")
def directory_add():
    if err := require_login():
        return err

    data = request.json or {}

    name = str(data.get("name") or "").strip()
    dtype = str(data.get("type") or "").strip().lower()

    if not name:
        return jsonify({"error": "missing_name"}), 400

    row = {
        "id": next_id("directory"),
        "type": dtype,
        "name": name,
        "email": (data.get("email") or "").strip(),
        "phone": (data.get("phone") or "").strip(),
        "address": (data.get("address") or "").strip(),
        "created_by": current_user()["username"],
        "created_at": now_iso()
    }

    append_row("directory", row)
    return jsonify(row)


# quick add (same logic)
@app.post("/api/directory/quick")
def directory_add_quick():
    return directory_add()
# ============================================================
#  ADMIN — CATEGORIES
# ============================================================

@app.get("/api/categories")
def get_categories():
    return jsonify(read_rows("categories"))


@app.post("/api/categories")
def add_category():
    if err := require_admin():
        return err

    data = request.json or {}
    name = (data.get("name") or "").strip()
    abbr = (data.get("abbr") or "").strip().upper()

    if not name or not abbr:
        return jsonify({"error": "missing_fields"}), 400

    rows = read_rows("categories")
    if any(str(r.get("abbr")).upper() == abbr for r in rows):
        return jsonify({"error": "duplicate_abbr"}), 409

    row = {
        "id": next_id("categories"),
        "name": name,
        "abbr": abbr,
        "created_at": now_iso()
    }
    append_row("categories", row)
    return jsonify(row)


@app.delete("/api/categories/<int:cid>")
def delete_category(cid):
    if err := require_admin():
        return err

    delete_row_by_id("categories", cid)
    return jsonify({"ok": True})


# ============================================================
#  ADMIN — VESSELS
# ============================================================

@app.get("/api/vessels")
def get_vessels():
    return jsonify(read_rows("vessels"))


@app.post("/api/vessels")
def add_vessel():
    if err := require_admin():
        return err

    data = request.json or {}
    name = (data.get("name") or "").strip()

    if not name:
        return jsonify({"error": "missing_name"}), 400

    row = {
        "id": next_id("vessels"),
        "name": name,
        "created_at": now_iso()
    }
    append_row("vessels", row)
    return jsonify(row)


@app.delete("/api/vessels/<int:vid>")
def delete_vessel(vid):
    if err := require_admin():
        return err

    delete_row_by_id("vessels", vid)
    return jsonify({"ok": True})


# ============================================================
#  ADMIN — USERS
# ============================================================

@app.get("/api/users")
def get_users():
    if err := require_admin():
        return err
    return jsonify(read_rows("users"))


@app.post("/api/users")
def add_user():
    if err := require_admin():
        return err

    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = (data.get("role") or "user").strip().lower()

    if not username or not password:
        return jsonify({"error": "missing_fields"}), 400

    if role not in ROLES:
        role = "user"

    rows = read_rows("users")
    if any(str(r.get("username")) == username for r in rows):
        return jsonify({"error": "duplicate"}), 409

    append_row("users", {
        "username": username,
        "password_hash": hash_pw(password),
        "role": role,
        "created_at": now_iso()
    })
    return jsonify({"ok": True})


@app.delete("/api/users/<username>")
def delete_user(username):
    if err := require_admin():
        return err

    if username == "admin":
        return jsonify({"error": "cannot_delete_root"}), 400

    wb = get_wb()
    ws = wb["users"]
    headers = [c.value for c in ws[1]]
    try:
        u_col = headers.index("username") + 1
    except ValueError:
        u_col = 1

    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, u_col).value) == username:
            ws.delete_rows(r)
            save_wb_safe(wb, DB_FILE)
            return jsonify({"ok": True})

    return jsonify({"error": "not_found"}), 404


# ============================================================
#  BACKUPS — LIST
# ============================================================

@app.get("/api/backups")
def list_backups():
    if err := require_admin():
        return err

    ensure_db()
    files = []
    if os.path.isdir(BACKUP_DIR):
        for f in os.listdir(BACKUP_DIR):
            if f.lower().endswith(".xlsx"):
                path = os.path.join(BACKUP_DIR, f)
                st = os.stat(path)
                files.append({
                    "name": f,
                    "size": st.st_size,
                    "created_at": datetime.utcfromtimestamp(st.st_mtime).isoformat()
                })
    files.sort(key=lambda x: x["name"], reverse=True)
    return jsonify(files)


# ============================================================
#  BACKUPS — CREATE & DOWNLOAD LATEST
#  (Used by "Download Backup" button → /api/backup)
# ============================================================

@app.get("/api/backup")
def create_backup():
    if err := require_admin():
        return err

    ensure_db()
    os.makedirs(BACKUP_DIR, exist_ok=True)

    name = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join(BACKUP_DIR, name)
    shutil.copy2(DB_FILE, path)
    return send_file(path, as_attachment=True, download_name=name)


# ============================================================
#  BACKUPS — DOWNLOAD SPECIFIC FILE
#  (Used by dl-backup button → GET /api/backups/<name>)
# ============================================================

@app.get("/api/backups/<name>")
def download_backup(name):
    if err := require_admin():
        return err

    safe_name = os.path.basename(name)
    path = os.path.join(BACKUP_DIR, safe_name)

    if not os.path.isfile(path):
        return jsonify({"error": "not_found"}), 404

    return send_file(path, as_attachment=True, download_name=safe_name)


# ============================================================
#  BACKUPS — RESTORE
#  (Used by restore-backup → POST /api/backups/<name>/restore)
# ============================================================

@app.post("/api/backups/<name>/restore")
def restore_backup(name):
    if err := require_admin():
        return err

    safe_name = os.path.basename(name)
    path = os.path.join(BACKUP_DIR, safe_name)

    if not os.path.isfile(path):
        return jsonify({"error": "not_found"}), 404

    shutil.copy2(path, DB_FILE)
    return jsonify({"ok": True})


# ============================================================
#  BACKUPS — DELETE
#  (Used by delete-backup → DELETE /api/backups/<name>)
# ============================================================

@app.delete("/api/backups/<name>")
def delete_backup(name):
    if err := require_admin():
        return err

    safe_name = os.path.basename(name)
    path = os.path.join(BACKUP_DIR, safe_name)

    if not os.path.isfile(path):
        return jsonify({"error": "not_found"}), 404

    os.remove(path)
    return jsonify({"ok": True})


# ============================================================
#  BACKUPS — UPLOAD & OVERWRITE DB
#  (Used by "Upload .xlsx (Overwrite)" → POST /api/upload)
# ============================================================

@app.post("/api/upload")
def upload_backup_overwrite():
    if err := require_admin():
        return err

    if "file" not in request.files:
        return jsonify({"error": "no_file"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "empty_filename"}), 400

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_UPLOAD_EXT:
        return jsonify({"error": "invalid_ext"}), 400

    # Save uploaded to a temp path first
    tmp_path = os.path.join(BACKUP_DIR, f"_upload_tmp_{int(time.time())}.xlsx")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    file.save(tmp_path)

    # Basic validation: try open with openpyxl
    try:
        _ = openpyxl.load_workbook(tmp_path)
    except Exception:
        os.remove(tmp_path)
        return jsonify({"error": "invalid_excel"}), 400

    # Overwrite main DB & also store a backup copy
    shutil.copy2(tmp_path, DB_FILE)

    stamped_name = f"uploaded_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    backup_copy = os.path.join(BACKUP_DIR, stamped_name)
    shutil.copy2(tmp_path, backup_copy)

    os.remove(tmp_path)
    return jsonify({"ok": True})


# ============================================================
#  MAIN ENTRY POINT
# ============================================================

if __name__ == "__main__":
    ensure_db()
    app.run(host="0.0.0.0", port=5000, debug=True)

