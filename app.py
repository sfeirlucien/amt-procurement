import os
import json
import time
import threading
from datetime import datetime
from functools import wraps

import requests
from flask import Flask, request, jsonify, session, send_from_directory
from werkzeug.security import generate_password_hash, check_password_hash
from openpyxl import Workbook, load_workbook

# ============================================================
# CONFIG
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

# Your Excel file name (keep same as your project)
EXCEL_FILE = os.path.join(BASE_DIR, "data.xlsx")

# Flask
app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = os.environ.get("SECRET_KEY", "amt-procurement-secret")

# ============================================================
# EXCEL STORAGE LAYER (no pandas)
# ============================================================

LOCK = threading.Lock()

SHEETS = {
    "requisitions": [
        "id", "number", "vessel", "category", "supplier",
        "date_ordered", "expected_date",
        "total_amount", "currency", "usd_amount",
        "paid", "delivered", "status", "created_at"
    ],
    "landings": [
        "id", "vessel", "description", "workshop",
        "expected_date", "landed_date",
        "amount", "currency", "usd_amount",
        "paid", "delivered", "status", "created_at"
    ],
    "directory": [
        "id", "type", "name", "email", "phone", "address", "created_at"
    ],
    "categories": [
        "id", "name", "abbr", "created_at"
    ],
    "vessels": [
        "id", "name", "created_at"
    ],
    "users": [
        "username", "password_hash", "role", "created_at"
    ]
}


def _ensure_workbook():
    """Create workbook + required sheets if missing."""
    if not os.path.exists(EXCEL_FILE):
        wb = Workbook()
        wb.remove(wb.active)  # remove default sheet
        for sheet_name, headers in SHEETS.items():
            ws = wb.create_sheet(sheet_name)
            ws.append(headers)
        wb.save(EXCEL_FILE)

    # Ensure all sheets exist with headers
    wb = load_workbook(EXCEL_FILE)
    changed = False
    for sheet_name, headers in SHEETS.items():
        if sheet_name not in wb.sheetnames:
            ws = wb.create_sheet(sheet_name)
            ws.append(headers)
            changed = True
        else:
            ws = wb[sheet_name]
            if ws.max_row == 0:
                ws.append(headers)
                changed = True
            else:
                first_row = [cell.value for cell in ws[1]]
                if first_row != headers:
                    # rewrite headers if mismatched
                    ws.delete_rows(1)
                    ws.insert_rows(1)
                    ws.append(headers)
                    changed = True
    if changed:
        wb.save(EXCEL_FILE)


def _read_sheet(sheet_name):
    _ensure_workbook()
    wb = load_workbook(EXCEL_FILE)
    ws = wb[sheet_name]
    headers = [c.value for c in ws[1]]
    rows = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        if all(v is None for v in r):
            continue
        obj = dict(zip(headers, r))
        rows.append(obj)
    return rows


def _write_sheet(sheet_name, rows):
    _ensure_workbook()
    wb = load_workbook(EXCEL_FILE)
    ws = wb[sheet_name]
    headers = SHEETS[sheet_name]

    # clear all rows except header
    ws.delete_rows(2, ws.max_row)
    for obj in rows:
        ws.append([obj.get(h) for h in headers])
    wb.save(EXCEL_FILE)


def _next_id(sheet_name):
    rows = _read_sheet(sheet_name)
    max_id = 0
    for r in rows:
        try:
            max_id = max(max_id, int(r.get("id") or 0))
        except:
            pass
    return max_id + 1


def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%d")


# ============================================================
# AUTH HELPERS
# ============================================================

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "username" not in session:
            return jsonify({"error": "login_required"}), 401
        return fn(*args, **kwargs)
    return wrapper


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "username" not in session:
            return jsonify({"error": "login_required"}), 401
        if session.get("role") != "admin":
            return jsonify({"error": "admin_required"}), 403
        return fn(*args, **kwargs)
    return wrapper


def ensure_default_admin():
    """Create a default admin if users sheet is empty."""
    users = _read_sheet("users")
    if not users:
        users.append({
            "username": "admin",
            "password_hash": generate_password_hash("admin123"),
            "role": "admin",
            "created_at": now_iso()
        })
        _write_sheet("users", users)


# ============================================================
# CURRENCY (USD conversion)
# ============================================================

RATES_CACHE = {"ts": 0, "rates": {"USD": 1.0}, "symbols": ["USD"]}

def fetch_rates():
    """
    Pull latest rates. No API key needed.
    Cached for 6 hours.
    """
    global RATES_CACHE
    if time.time() - RATES_CACHE["ts"] < 6 * 3600:
        return RATES_CACHE["rates"]

    try:
        # free endpoint
        resp = requests.get("https://open.er-api.com/v6/latest/USD", timeout=10)
        data = resp.json()
        rates = data.get("rates", {})
        if rates:
            RATES_CACHE = {
                "ts": time.time(),
                "rates": rates,
                "symbols": sorted(list(rates.keys()))
            }
            return rates
    except:
        pass

    # fallback minimal set
    return RATES_CACHE["rates"]


def to_usd(amount, currency):
    try:
        amount = float(amount or 0)
    except:
        amount = 0.0
    currency = (currency or "USD").upper()

    rates = fetch_rates()
    rate = rates.get(currency)
    if not rate or rate == 0:
        return amount  # if unknown currency, keep same
    # rates are BASE USD => 1 USD = rate * currency
    # so currency to USD = amount / rate
    return amount / rate


# ============================================================
# ROUTES: SERVE INDEX FROM /static
# ============================================================

@app.route("/")
def index():
    # Your index.html is in /static
    return send_from_directory(STATIC_DIR, "index.html")


# ============================================================
# SESSION / LOGIN
# ============================================================

@app.route("/api/session")
def api_session():
    if "username" in session:
        return jsonify({
            "username": session["username"],
            "role": session.get("role", "user")
        })
    return jsonify({})


@app.route("/api/login", methods=["POST"])
def api_login():
    ensure_default_admin()
    data = request.get_json(force=True)
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    users = _read_sheet("users")
    user = next((u for u in users if u["username"] == username), None)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "invalid_credentials"}), 401

    session["username"] = username
    session["role"] = user.get("role", "user")
    return jsonify({"ok": True})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})


# ============================================================
# USERS (ADMIN)
# ============================================================

@app.route("/api/users", methods=["GET"])
@admin_required
def list_users():
    users = _read_sheet("users")
    safe = [{"username": u["username"], "role": u.get("role", "user")} for u in users]
    return jsonify(safe)


@app.route("/api/users", methods=["POST"])
@admin_required
def add_user():
    data = request.get_json(force=True)
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = data.get("role") or "user"

    if not username or not password:
        return jsonify({"error": "missing_fields"}), 400

    users = _read_sheet("users")
    if any(u["username"] == username for u in users):
        return jsonify({"error": "duplicate_username"}), 400

    users.append({
        "username": username,
        "password_hash": generate_password_hash(password),
        "role": role,
        "created_at": now_iso()
    })
    _write_sheet("users", users)
    return jsonify({"ok": True})


@app.route("/api/users/<username>", methods=["DELETE"])
@admin_required
def delete_user(username):
    users = _read_sheet("users")
    users = [u for u in users if u["username"] != username]
    _write_sheet("users", users)
    return jsonify({"ok": True})


# ============================================================
# CATEGORIES
# ============================================================

@app.route("/api/categories", methods=["GET"])
@login_required
def list_categories():
    cats = _read_sheet("categories")
    return jsonify(cats)


@app.route("/api/categories", methods=["POST"])
@admin_required
def add_category():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    abbr = (data.get("abbr") or "").strip()

    if not name or not abbr:
        return jsonify({"error": "missing_fields"}), 400

    cats = _read_sheet("categories")
    if any(c["abbr"] == abbr for c in cats):
        return jsonify({"error": "duplicate_abbr"}), 400

    cats.append({
        "id": _next_id("categories"),
        "name": name,
        "abbr": abbr,
        "created_at": now_iso()
    })
    _write_sheet("categories", cats)
    return jsonify({"ok": True})


@app.route("/api/categories/<int:cid>", methods=["PATCH"])
@admin_required
def edit_category(cid):
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    abbr = (data.get("abbr") or "").strip()

    cats = _read_sheet("categories")
    for c in cats:
        if int(c["id"]) == cid:
            if abbr and any(x["abbr"] == abbr and int(x["id"]) != cid for x in cats):
                return jsonify({"error": "duplicate_abbr"}), 400
            if name:
                c["name"] = name
            if abbr:
                c["abbr"] = abbr
            _write_sheet("categories", cats)
            return jsonify({"ok": True})

    return jsonify({"error": "not_found"}), 404


@app.route("/api/categories/<int:cid>", methods=["DELETE"])
@admin_required
def delete_category(cid):
    cats = _read_sheet("categories")
    cats = [c for c in cats if int(c["id"]) != cid]
    _write_sheet("categories", cats)
    return jsonify({"ok": True})


# ============================================================
# VESSELS
# ============================================================

@app.route("/api/vessels", methods=["GET"])
@login_required
def list_vessels():
    v = _read_sheet("vessels")
    return jsonify(v)


@app.route("/api/vessels", methods=["POST"])
@admin_required
def add_vessel():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "missing_fields"}), 400

    vessels = _read_sheet("vessels")
    if any((x.get("name") or "").strip().lower() == name.lower() for x in vessels):
        return jsonify({"error": "duplicate_name"}), 400

    vessels.append({
        "id": _next_id("vessels"),
        "name": name,
        "created_at": now_iso()
    })
    _write_sheet("vessels", vessels)
    return jsonify({"ok": True})


@app.route("/api/vessels/<int:vid>", methods=["PATCH"])
@admin_required
def edit_vessel(vid):
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()

    vessels = _read_sheet("vessels")
    for v in vessels:
        if int(v["id"]) == vid:
            if name and any((x.get("name") or "").lower() == name.lower() and int(x["id"]) != vid for x in vessels):
                return jsonify({"error": "duplicate_name"}), 400
            if name:
                v["name"] = name
            _write_sheet("vessels", vessels)
            return jsonify({"ok": True})

    return jsonify({"error": "not_found"}), 404


@app.route("/api/vessels/<int:vid>", methods=["DELETE"])
@admin_required
def delete_vessel(vid):
    vessels = _read_sheet("vessels")
    vessels = [v for v in vessels if int(v["id"]) != vid]
    _write_sheet("vessels", vessels)
    return jsonify({"ok": True})


# ============================================================
# DIRECTORY
# ============================================================

@app.route("/api/directory", methods=["GET"])
@login_required
def list_directory():
    rows = _read_sheet("directory")
    t = request.args.get("type")
    if t:
        rows = [r for r in rows if (r.get("type") or "").lower() == t.lower()]
    return jsonify(rows)


@app.route("/api/directory/quick", methods=["POST"])
@login_required
def add_directory_quick():
    data = request.get_json(force=True)
    rtype = (data.get("type") or "").strip().lower()
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip()
    phone = (data.get("phone") or "").strip()
    address = (data.get("address") or "").strip()

    if rtype not in ["supplier", "workshop"]:
        return jsonify({"error": "invalid_type"}), 400
    if not name:
        return jsonify({"error": "missing_fields"}), 400

    rows = _read_sheet("directory")
    rows.append({
        "id": _next_id("directory"),
        "type": rtype,
        "name": name,
        "email": email,
        "phone": phone,
        "address": address,
        "created_at": now_iso()
    })
    _write_sheet("directory", rows)
    return jsonify({"ok": True})


# ============================================================
# REQUISITIONS
# ============================================================

@app.route("/api/requisitions", methods=["GET"])
@login_required
def list_requisitions():
    rows = _read_sheet("requisitions")
    return jsonify(rows)


@app.route("/api/requisitions", methods=["POST"])
@login_required
def add_requisition():
    data = request.get_json(force=True)

    number = (data.get("number") or "").strip()
    vessel = (data.get("vessel") or "").strip()
    category = (data.get("category") or "").strip()
    supplier = (data.get("supplier") or "").strip()
    date_ordered = data.get("date_ordered") or ""
    expected_date = data.get("expected_date") or ""
    total_amount = float(data.get("total_amount") or 0)
    currency = (data.get("currency") or "USD").upper()
    paid = int(data.get("paid") or 0)
    delivered = int(data.get("delivered") or 0)
    status = (data.get("status") or "open").lower()

    usd_amount = round(to_usd(total_amount, currency), 4)

    rows = _read_sheet("requisitions")
    rid = _next_id("requisitions")
    rows.append({
        "id": rid,
        "number": number,
        "vessel": vessel,
        "category": category,
        "supplier": supplier,
        "date_ordered": date_ordered,
        "expected_date": expected_date,
        "total_amount": total_amount,
        "currency": currency,
        "usd_amount": usd_amount,
        "paid": paid,
        "delivered": delivered,
        "status": status,
        "created_at": now_iso()
    })
    _write_sheet("requisitions", rows)
    return jsonify({"ok": True, "id": rid})


@app.route("/api/requisitions/<int:rid>", methods=["PATCH"])
@login_required
def edit_requisition(rid):
    data = request.get_json(force=True)
    rows = _read_sheet("requisitions")

    for r in rows:
        if int(r["id"]) == rid:
            for k in [
                "number", "vessel", "category", "supplier",
                "date_ordered", "expected_date",
                "total_amount", "currency", "paid", "delivered", "status"
            ]:
                if k in data:
                    r[k] = data[k]

            # recompute usd if amount/currency changed
            amt = float(r.get("total_amount") or 0)
            cur = (r.get("currency") or "USD").upper()
            r["currency"] = cur
            r["usd_amount"] = round(to_usd(amt, cur), 4)

            _write_sheet("requisitions", rows)
            return jsonify({"ok": True})

    return jsonify({"error": "not_found"}), 404


@app.route("/api/requisitions/<int:rid>", methods=["DELETE"])
@login_required
def delete_requisition(rid):
    # soft-delete: mark cancelled
    rows = _read_sheet("requisitions")
    for r in rows:
        if int(r["id"]) == rid:
            r["status"] = "cancelled"
            r["delivered"] = 0
            _write_sheet("requisitions", rows)
            return jsonify({"ok": True})
    return jsonify({"error": "not_found"}), 404


# ============================================================
# LANDINGS
# ============================================================

@app.route("/api/landings", methods=["GET"])
@login_required
def list_landings():
    rows = _read_sheet("landings")
    return jsonify(rows)


@app.route("/api/landings", methods=["POST"])
@login_required
def add_landing():
    data = request.get_json(force=True)

    vessel = (data.get("vessel") or "").strip()
    description = (data.get("description") or "").strip()
    workshop = (data.get("workshop") or "").strip()
    expected_date = data.get("expected_date") or ""
    landed_date = data.get("landed_date") or ""
    amount = float(data.get("amount") or 0)
    currency = (data.get("currency") or "USD").upper()
    paid = int(data.get("paid") or 0)
    delivered = int(data.get("delivered") or 0)
    status = (data.get("status") or "open").lower()

    usd_amount = round(to_usd(amount, currency), 4)

    rows = _read_sheet("landings")
    lid = _next_id("landings")
    rows.append({
        "id": lid,
        "vessel": vessel,
        "description": description,
        "workshop": workshop,
        "expected_date": expected_date,
        "landed_date": landed_date,
        "amount": amount,
        "currency": currency,
        "usd_amount": usd_amount,
        "paid": paid,
        "delivered": delivered,
        "status": status,
        "created_at": now_iso()
    })
    _write_sheet("landings", rows)
    return jsonify({"ok": True, "id": lid})


@app.route("/api/landings/<int:lid>", methods=["PATCH"])
@login_required
def edit_landing(lid):
    data = request.get_json(force=True)
    rows = _read_sheet("landings")

    for r in rows:
        if int(r["id"]) == lid:
            for k in [
                "vessel", "description", "workshop",
                "expected_date", "landed_date",
                "amount", "currency", "paid", "delivered", "status"
            ]:
                if k in data:
                    r[k] = data[k]

            amt = float(r.get("amount") or 0)
            cur = (r.get("currency") or "USD").upper()
            r["currency"] = cur
            r["usd_amount"] = round(to_usd(amt, cur), 4)

            _write_sheet("landings", rows)
            return jsonify({"ok": True})

    return jsonify({"error": "not_found"}), 404


@app.route("/api/landings/<int:lid>", methods=["DELETE"])
@login_required
def delete_landing(lid):
    rows = _read_sheet("landings")
    for r in rows:
        if int(r["id"]) == lid:
            r["status"] = "cancelled"
            r["delivered"] = 0
            _write_sheet("landings", rows)
            return jsonify({"ok": True})
    return jsonify({"error": "not_found"}), 404


# ============================================================
# CURRENCIES ENDPOINT
# ============================================================

@app.route("/api/currencies", methods=["GET"])
@login_required
def api_currencies():
    rates = fetch_rates()
    symbols = sorted(list(rates.keys()))
    if "USD" not in symbols:
        symbols.insert(0, "USD")
    return jsonify({"currencies": symbols})


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    _ensure_workbook()
    ensure_default_admin()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
