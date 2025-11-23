"""
AMT Procurement - Single-file Flask Backend (Excel DB)
Rebuilt from scratch per specs.

Features:
- Excel-based storage (office_ops.xlsx auto-created)
- Roles: admin, user, viewer, finance
- Default admin if none exists: admin / admin123
- Manual backups only
- Requisitions + Landings + Directory + Categories + Vessels + Users
- Real-time FX conversion to USD (cached)
- Permissions:
    * admin: full CRUD
    * other roles: add/edit own requisitions/landings, no delete, no admin access
"""

import os
import json
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
import openpyxl
from openpyxl import Workbook
from flask import Flask, jsonify, request, session, send_from_directory
from flask_cors import CORS


# -------------------------------------------------
# App init
# -------------------------------------------------
app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = os.environ.get("SECRET_KEY", "AMT_SECRET_KEY_CHANGE_ME")

# Allow local dev + Render domain. If you later host frontend elsewhere, add it here.
CORS(app, supports_credentials=True, origins=[
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "https://amt-procurement.onrender.com",
])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ✅ IMPORTANT: use your real Excel file name
DB_FILE = os.path.join(BASE_DIR, "office_ops.xlsx")

DEFAULT_ADMIN = {"username": "admin", "password": "admin123", "role": "admin"}
ROLES = {"admin", "user", "viewer", "finance"}

FX_CACHE_FILE = os.path.join(BASE_DIR, "fx_cache.json")
FX_CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 hours


SHEETS: Dict[str, List[str]] = {
    "users": ["username", "password_hash", "role", "created_at"],
    "requisitions": [
        "id", "number", "vessel", "category", "supplier",
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
    """
    Create office_ops.xlsx and sheets if not exist.
    Also create default admin if:
      - users sheet is empty OR
      - users sheet exists but all password_hash are empty/None.
    """
    if not os.path.exists(DB_FILE):
        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            wb.remove(wb["Sheet"])
        for sname, headers in SHEETS.items():
            ws = wb.create_sheet(sname)
            ws.append(headers)
        wb.save(DB_FILE)

    # ensure at least one valid admin exists
    users = read_rows("users")

    def has_valid_admin(us: List[Dict[str, Any]]) -> bool:
        for u in us:
            if (u.get("role") or "").lower() == "admin" and (u.get("password_hash") or "").strip():
                return True
        return False

    if (not users) or (not has_valid_admin(users)):
        append_row("users", {
            "username": DEFAULT_ADMIN["username"],
            "password_hash": hash_pw(DEFAULT_ADMIN["password"]),
            "role": DEFAULT_ADMIN["role"],
            "created_at": now_iso()
        })


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
    append_row("logs", {"timestamp": now_iso(), "action": action, "details": details})


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
        r = requests.get(
            "https://api.exchangerate.host/latest",
            params={"base": base},
            timeout=10
        )
        data = r.json()
        rates = data.get("rates") or {}
        if rates:
            save_fx_cache({
                "timestamp": datetime.utcnow().timestamp(),
                "base": base,
                "rates": rates
            })
            return rates
    except Exception:
        pass

    if cache.get("rates"):
        return cache["rates"]
    return {"USD": 1.0, "EUR": 0.9, "AED": 3.67, "GBP": 0.78, "LBP": 90000.0}


def currencies_list() -> List[str]:
    rates = fetch_fx_rates("USD")
    return sorted(set(rates.keys()) | {"USD"})


def to_usd(amount: float, currency: str) -> float:
    currency = (currency or "USD").upper()
    if currency == "USD":
        return float(amount)
    rates = fetch_fx_rates("USD")
    r = rates.get(currency)
    if not r or r == 0:
        return float(amount)
    return float(amount) / float(r)


# -------------------------------------------------
# Static / health
# -------------------------------------------------
@app.get("/")
def home():
    return send_from_directory("static", "index.html")


@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "time": now_iso()})


# -------------------------------------------------
# Auth routes
# -------------------------------------------------
@app.post("/api/login")
def login():
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    users = read_rows("users")
    u = next((x for x in users if x.get("username") == username), None)

    if not u:
        return jsonify({"error": "invalid_credentials"}), 401

    stored_hash = (u.get("password_hash") or "").strip()
    if (not stored_hash) or (stored_hash != hash_pw(password)):
        return jsonify({"error": "invalid_credentials"}), 401

    session["username"] = username
    session["role"] = (u.get("role") or "user").lower()
    log_action("login", username)
    return jsonify({"ok": True, "username": username, "role": session["role"]})


@app.post("/api/logout")
def logout():
    u = current_user()
    session.clear()
    if u:
        log_action("logout", u["username"])
    return jsonify({"ok": True})


@app.get("/api/session")
def session_info():
    u = current_user()
    if not u:
        return jsonify({"logged_in": False}), 401
    return jsonify({"logged_in": True, **u})


# -------------------------------------------------
# FX / currencies routes
# -------------------------------------------------
@app.get("/api/currencies")
def api_currencies():
    return jsonify({"currencies": currencies_list()})


@app.get("/api/fx")
def api_fx():
    return jsonify({"base": "USD", "rates": fetch_fx_rates("USD")})


# -------------------------------------------------
# Categories (admin)
# -------------------------------------------------
@app.get("/api/categories")
def get_categories():
    return jsonify(read_rows("categories"))


@app.post("/api/categories")
def add_category():
    guard = require_admin()
    if guard:
        return guard
    data = request.json or {}
    name = (data.get("name") or "").strip()
    abbr = (data.get("abbr") or "").strip().upper()
    if not name or not abbr:
        return jsonify({"error": "missing_fields"}), 400

    cats = read_rows("categories")
    if any((c.get("abbr") or "").upper() == abbr for c in cats):
        return jsonify({"error": "duplicate_abbr"}), 409

    row = {
        "id": next_id("categories"),
        "name": name,
        "abbr": abbr,
        "created_at": now_iso()
    }
    append_row("categories", row)
    log_action("category_add", abbr)
    return jsonify(row)


@app.patch("/api/categories/<int:cid>")
def edit_category(cid: int):
    guard = require_admin()
    if guard:
        return guard
    data = request.json or {}
    updates = {}

    if "name" in data:
        updates["name"] = (data["name"] or "").strip()
    if "abbr" in data:
        updates["abbr"] = (data["abbr"] or "").strip().upper()

    if not updates:
        return jsonify({"error": "no_updates"}), 400

    cats = read_rows("categories")
    if "abbr" in updates:
        if any(c["id"] != cid and (c.get("abbr") or "").upper() == updates["abbr"] for c in cats):
            return jsonify({"error": "duplicate_abbr"}), 409

    if not update_row_by_id("categories", cid, updates):
        return jsonify({"error": "not_found"}), 404

    log_action("category_edit", str(cid))
    return jsonify({"ok": True})


@app.delete("/api/categories/<int:cid>")
def delete_category(cid: int):
    guard = require_admin()
    if guard:
        return guard
    if not delete_row_by_id("categories", cid):
        return jsonify({"error": "not_found"}), 404
    log_action("category_delete", str(cid))
    return jsonify({"ok": True})


# -------------------------------------------------
# Vessels (admin)
# -------------------------------------------------
@app.get("/api/vessels")
def get_vessels():
    return jsonify(read_rows("vessels"))


@app.post("/api/vessels")
def add_vessel():
    guard = require_admin()
    if guard:
        return guard
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "missing_fields"}), 400

    vs = read_rows("vessels")
    if any((v.get("name") or "").strip().lower() == name.lower() for v in vs):
        return jsonify({"error": "duplicate_name"}), 409

    row = {"id": next_id("vessels"), "name": name, "created_at": now_iso()}
    append_row("vessels", row)
    log_action("vessel_add", name)
    return jsonify(row)


@app.patch("/api/vessels/<int:vid>")
def edit_vessel(vid: int):
    guard = require_admin()
    if guard:
        return guard
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "missing_fields"}), 400

    vs = read_rows("vessels")
    if any(v["id"] != vid and (v.get("name") or "").strip().lower() == name.lower() for v in vs):
        return jsonify({"error": "duplicate_name"}), 409

    if not update_row_by_id("vessels", vid, {"name": name}):
        return jsonify({"error": "not_found"}), 404

    log_action("vessel_edit", str(vid))
    return jsonify({"ok": True})


@app.delete("/api/vessels/<int:vid>")
def delete_vessel(vid: int):
    guard = require_admin()
    if guard:
        return guard
    if not delete_row_by_id("vessels", vid):
        return jsonify({"error": "not_found"}), 404
    log_action("vessel_delete", str(vid))
    return jsonify({"ok": True})


# -------------------------------------------------
# Users (admin)
# -------------------------------------------------
@app.get("/api/users")
def list_users():
    guard = require_admin()
    if guard:
        return guard
    users = read_rows("users")
    return jsonify([{"username": u["username"], "role": u.get("role", "user")} for u in users])


@app.post("/api/users")
def add_user():
    guard = require_admin()
    if guard:
        return guard
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = (data.get("role") or "user").strip().lower()

    if not username or not password or role not in ROLES:
        return jsonify({"error": "missing_fields"}), 400

    users = read_rows("users")
    if any(u["username"] == username for u in users):
        return jsonify({"error": "duplicate_username"}), 409

    append_row("users", {
        "username": username,
        "password_hash": hash_pw(password),
        "role": role,
        "created_at": now_iso()
    })
    log_action("user_add", username)
    return jsonify({"ok": True})


@app.delete("/api/users/<username>")
def delete_user(username: str):
    guard = require_admin()
    if guard:
        return guard
    if username == "admin":
        return jsonify({"error": "cannot_delete_admin"}), 400

    wb = get_wb()
    ws = wb["users"]
    headers = [c.value for c in ws[1]]
    u_col = headers.index("username") + 1
    for r_idx in range(2, ws.max_row + 1):
        if ws.cell(r_idx, u_col).value == username:
            ws.delete_rows(r_idx, 1)
            wb.save(DB_FILE)
            log_action("user_delete", username)
            return jsonify({"ok": True})

    return jsonify({"error": "not_found"}), 404


# -------------------------------------------------
# Directory (suppliers / workshops)
# -------------------------------------------------
@app.get("/api/directory")
def list_directory():
    dtype = request.args.get("type")
    rows = read_rows("directory")
    if dtype:
        rows = [r for r in rows if (r.get("type") or "").lower() == dtype.lower()]
    return jsonify(rows)


@app.post("/api/directory")
def add_directory():
    guard = require_login()
    if guard:
        return guard
    data = request.json or {}
    dtype = (data.get("type") or "").strip().lower()
    name = (data.get("name") or "").strip()

    if dtype not in {"supplier", "workshop"} or not name:
        return jsonify({"error": "missing_fields"}), 400

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
    log_action("directory_add", f"{dtype}:{name}")
    return jsonify(row)


@app.post("/api/directory/quick")
def add_directory_quick():
    return add_directory()


# -------------------------------------------------
# Requisitions
# -------------------------------------------------
@app.get("/api/requisitions")
def list_requisitions():
    rows = read_rows("requisitions")
    for r in rows:
        r["amount_usd"] = float(r.get("amount_usd") or 0)
        r["amount_original"] = float(r.get("amount_original") or 0)
        r["paid"] = int(r.get("paid") or 0)
        r["delivered"] = int(r.get("delivered") or 0)
    return jsonify(rows)


@app.post("/api/requisitions")
def add_requisition():
    guard = require_login()
    if guard:
        return guard

    data = request.json or {}
    number = (data.get("number") or "").strip()
    vessel = (data.get("vessel") or "").strip()
    category = (data.get("category") or "").strip().upper()
    supplier = (data.get("supplier") or "").strip()
    date_ordered = data.get("date_ordered") or ""
    expected = data.get("expected") or ""
    amount = float(data.get("amount") or 0)
    currency = (data.get("currency") or "USD").strip().upper()
    paid = 1 if data.get("paid") else 0
    delivered = 1 if data.get("delivered") else 0
    status = (data.get("status") or "open").strip().lower()
    po_number = (data.get("po_number") or "").strip()
    remarks = (data.get("remarks") or "").strip()
    urgency = (data.get("urgency") or "normal").strip().lower()

    if not number or not vessel or not category or not supplier or not date_ordered or amount <= 0:
        return jsonify({"error": "missing_fields"}), 400

    cats = read_rows("categories")
    if not any((c.get("abbr") or "").upper() == category for c in cats):
        return jsonify({"error": "unknown_category"}), 400

    reqs = read_rows("requisitions")
    if any((r.get("number") or "") == number for r in reqs):
        return jsonify({"error": "duplicate_number"}), 409

    usd = round(to_usd(amount, currency), 2)

    row = {
        "id": next_id("requisitions"),
        "number": number,
        "vessel": vessel,
        "category": category,
        "supplier": supplier,
        "date_ordered": date_ordered,
        "expected": expected,
        "amount_original": amount,
        "currency": currency,
        "amount_usd": usd,
        "paid": paid,
        "delivered": delivered,
        "status": status,
        "po_number": po_number,
        "remarks": remarks,
        "urgency": urgency,
        "created_by": current_user()["username"],
        "created_at": now_iso(),
        "updated_at": now_iso()
    }
    append_row("requisitions", row)
    log_action("requisition_add", number)
    return jsonify(row)


@app.patch("/api/requisitions/<int:rid>")
def edit_requisition(rid: int):
    guard = require_login()
    if guard:
        return guard

    reqs = read_rows("requisitions")
    row = next((r for r in reqs if int(r["id"]) == rid), None)
    if not row:
        return jsonify({"error": "not_found"}), 404
    if not can_edit_row(row):
        return jsonify({"error": "not_allowed"}), 403

    data = request.json or {}
    updates: Dict[str, Any] = {}

    for k in ["number", "vessel", "category", "supplier", "date_ordered",
              "expected", "status", "po_number", "remarks", "urgency"]:
        if k in data:
            val = data[k]
            if isinstance(val, str):
                val = val.strip()
            if k == "category":
                val = val.upper()
            updates[k] = val

    if "paid" in data:
        updates["paid"] = 1 if data["paid"] else 0
    if "delivered" in data:
        updates["delivered"] = 1 if data["delivered"] else 0

    if "amount" in data or "currency" in data:
        amount_new = float(data.get("amount") or row.get("amount_original") or 0)
        currency_new = (data.get("currency") or row.get("currency") or "USD").upper()
        updates["amount_original"] = amount_new
        updates["currency"] = currency_new
        updates["amount_usd"] = round(to_usd(amount_new, currency_new), 2)

    updates["updated_at"] = now_iso()

    if not update_row_by_id("requisitions", rid, updates):
        return jsonify({"error": "not_found"}), 404

    log_action("requisition_edit", str(rid))
    return jsonify({"ok": True})


@app.patch("/api/requisitions/<int:rid>/toggle_paid")
def toggle_paid_req(rid: int):
    guard = require_login()
    if guard:
        return guard

    reqs = read_rows("requisitions")
    row = next((r for r in reqs if int(r["id"]) == rid), None)
    if not row:
        return jsonify({"error": "not_found"}), 404
    if not can_edit_row(row):
        return jsonify({"error": "not_allowed"}), 403

    new_paid = 0 if int(row.get("paid") or 0) else 1
    update_row_by_id("requisitions", rid, {"paid": new_paid, "updated_at": now_iso()})
    log_action("requisition_toggle_paid", str(rid))
    return jsonify({"ok": True})


@app.delete("/api/requisitions/<int:rid>")
def delete_requisition(rid: int):
    guard = require_login()
    if guard:
        return guard
    u = current_user()
    if not u or u["role"] != "admin":
        return jsonify({"error": "not_allowed"}), 403

    if not delete_row_by_id("requisitions", rid):
        return jsonify({"error": "not_found"}), 404
    log_action("requisition_delete", str(rid))
    return jsonify({"ok": True})


# -------------------------------------------------
# Landings
# -------------------------------------------------
@app.get("/api/landings")
def list_landings():
    rows = read_rows("landings")
    for r in rows:
        r["amount_usd"] = float(r.get("amount_usd") or 0)
        r["amount_original"] = float(r.get("amount_original") or 0)
        r["paid"] = int(r.get("paid") or 0)
        r["delivered"] = int(r.get("delivered") or 0)
    return jsonify(rows)


@app.post("/api/landings")
def add_landing():
    guard = require_login()
    if guard:
        return guard

    data = request.json or {}
    vessel = (data.get("vessel") or "").strip()
    item = (data.get("item") or data.get("description") or "").strip()
    workshop = (data.get("workshop") or "").strip()
    expected = data.get("expected") or ""
    landed_date = data.get("landed_date") or ""
    amount = float(data.get("amount") or 0)
    currency = (data.get("currency") or "USD").strip().upper()
    paid = 1 if data.get("paid") else 0
    delivered = 1 if data.get("delivered") else 0
    status = (data.get("status") or "open").strip().lower()

    if not vessel or not item or not workshop or amount <= 0:
        return jsonify({"error": "missing_fields"}), 400

    usd = round(to_usd(amount, currency), 2)

    row = {
        "id": next_id("landings"),
        "vessel": vessel,
        "item": item,
        "workshop": workshop,
        "expected": expected,
        "landed_date": landed_date,
        "amount_original": amount,
        "currency": currency,
        "amount_usd": usd,
        "paid": paid,
        "delivered": delivered,
        "status": status,
        "created_by": current_user()["username"],
        "created_at": now_iso(),
        "updated_at": now_iso()
    }
    append_row("landings", row)
    log_action("landing_add", f"{vessel}:{item}")
    return jsonify(row)


@app.patch("/api/landings/<int:lid>")
def edit_landing(lid: int):
    guard = require_login()
    if guard:
        return guard

    lands = read_rows("landings")
    row = next((r for r in lands if int(r["id"]) == lid), None)
    if not row:
        return jsonify({"error": "not_found"}), 404
    if not can_edit_row(row):
        return jsonify({"error": "not_allowed"}), 403

    data = request.json or {}
    updates: Dict[str, Any] = {}

    for k in ["vessel", "item", "workshop", "expected", "landed_date", "status"]:
        if k in data:
            val = data[k]
            if isinstance(val, str):
                val = val.strip()
            updates[k] = val

    if "paid" in data:
        updates["paid"] = 1 if data["paid"] else 0
    if "delivered" in data:
        updates["delivered"] = 1 if data["delivered"] else 0

    if "amount" in data or "currency" in data:
        amount_new = float(data.get("amount") or row.get("amount_original") or 0)
        currency_new = (data.get("currency") or row.get("currency") or "USD").upper()
        updates["amount_original"] = amount_new
        updates["currency"] = currency_new
        updates["amount_usd"] = round(to_usd(amount_new, currency_new), 2)

    updates["updated_at"] = now_iso()

    if not update_row_by_id("landings", lid, updates):
        return jsonify({"error": "not_found"}), 404

    log_action("landing_edit", str(lid))
    return jsonify({"ok": True})


@app.patch("/api/landings/<int:lid>/toggle_paid")
def toggle_paid_land(lid: int):
    guard = require_login()
    if guard:
        return guard

    lands = read_rows("landings")
    row = next((r for r in lands if int(r["id"]) == lid), None)
    if not row:
        return jsonify({"error": "not_found"}), 404
    if not can_edit_row(row):
        return jsonify({"error": "not_allowed"}), 403

    new_paid = 0 if int(row.get("paid") or 0) else 1
    update_row_by_id("landings", lid, {"paid": new_paid, "updated_at": now_iso()})
    log_action("landing_toggle_paid", str(lid))
    return jsonify({"ok": True})


@app.delete("/api/landings/<int:lid>")
def delete_landing(lid: int):
    guard = require_login()
    if guard:
        return guard
    u = current_user()
    if not u or u["role"] != "admin":
        return jsonify({"error": "not_allowed"}), 403

    if not delete_row_by_id("landings", lid):
        return jsonify({"error": "not_found"}), 404
    log_action("landing_delete", str(lid))
    return jsonify({"ok": True})


# -------------------------------------------------
# Run local
# -------------------------------------------------
if __name__ == "__main__":
    ensure_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
