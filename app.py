# app.py
from flask import Flask, request, jsonify, session, send_from_directory
from werkzeug.security import generate_password_hash, check_password_hash
from openpyxl import Workbook, load_workbook
from pathlib import Path
import os, threading, time

# Try to use requests for live FX, fallback to static if missing
try:
    import requests
except ImportError:
    requests = None



app = Flask(__name__, static_url_path="/static", static_folder="static")
app.secret_key = os.environ.get("SECRET_KEY", "change-me")

# ===== Excel file location
DATA_DIR = Path(os.environ.get("OPS_DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
XLSX_PATH = DATA_DIR / "office_ops.xlsx"

WB_LOCK = threading.Lock()

SHEETS = {
    "Users":        ["username", "password_hash", "role"],
    "Directory":    ["id", "type", "name", "email", "phone", "address"],
    # Requisitions: store amount in USD, plus original amount + currency
    "Requisitions": [
        "id", "number", "vessel", "supplier",
        "date_ordered", "expected",
        "total_amount", "paid",
        "category", "delivered", "status",
        "currency", "original_amount",
    ],
    # Landings: store amount in USD, plus original amount + currency
    "Landings": [
        "id", "vessel", "item", "workshop",
        "amount", "paid",
        "expected", "landed_date", "status", "delivered",
        "currency", "amount_original",
    ],
    "Categories":   ["id", "name", "abbr"],
}

FX_CACHE = {"timestamp": 0, "rates": {}}

# ========= FX helpers =========
def _get_fx_rates():
    """
    Get FX rates (base = USD) from the internet, cached for 1 hour.
    If requests is not available or call fails, fallback to USD=1 only.
    """
    now = time.time()
    if FX_CACHE["rates"] and now - FX_CACHE["timestamp"] < 3600:
        return FX_CACHE["rates"]

    if not requests:
        FX_CACHE["rates"] = {"USD": 1.0}
        FX_CACHE["timestamp"] = now
        return FX_CACHE["rates"]

    try:
        resp = requests.get("https://api.exchangerate.host/latest?base=USD", timeout=5)
        data = resp.json()
        rates = data.get("rates", {})
        if not rates:
            raise ValueError("no rates")
        rates["USD"] = 1.0
        FX_CACHE["rates"] = rates
        FX_CACHE["timestamp"] = now
        return FX_CACHE["rates"]
    except Exception:
        # fallback: if nothing yet, at least keep USD=1
        if not FX_CACHE["rates"]:
            FX_CACHE["rates"] = {"USD": 1.0}
            FX_CACHE["timestamp"] = now
        return FX_CACHE["rates"]

STATIC_CURRENCY_TO_USD = {
    # 1 unit of currency ≈ this many USD (approximate fallback)
    "USD": 1.0,
    "EUR": 1.08,     # 1 EUR ≈ 1.08 USD
    "AED": 0.27,     # 1 AED ≈ 0.27 USD
    "GBP": 1.25,     # 1 GBP ≈ 1.25 USD
    "LBP": 0.000011, # 1 LBP ≈ 0.000011 USD (just an example)
    "SAR": 0.27,
    "QAR": 0.27,
    "KWD": 3.25,
    "OMR": 2.60,
    "CHF": 1.10,
}


def convert_to_usd(amount, currency):
    """
    Convert amount in <currency> to USD using latest FX.
    If unknown currency or FX fails, use static fallback.
    """
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return 0.0

    currency = (currency or "USD").upper()
    if currency == "USD":
        return amount

    # 1) Try live FX from exchangerate.host
    rates = _get_fx_rates()
    r = rates.get(currency)
    if r:
        # API gives: 1 USD = r * CURRENCY => 1 CURRENCY = 1/r USD
        return amount / float(r)

    # 2) Fallback: static approximate mapping (1 currency ≈ x USD)
    if currency in STATIC_CURRENCY_TO_USD:
        return amount * STATIC_CURRENCY_TO_USD[currency]

    # 3) Last resort: no conversion
    return amount
# ========= Workbook helpers =========
def _upgrade_schema(wb):
    """
    Option A: automatically upgrade existing Excel to have all required sheets/columns.
    Called for both new and existing workbooks.
    """
    changed = False

    # Ensure all sheets exist with at least headers
    for title, headers in SHEETS.items():
        if title not in wb.sheetnames:
            ws = wb.create_sheet(title)
            ws.append(headers)
            changed = True
        else:
            ws = wb[title]
            # if sheet is empty, write headers
            if ws.max_row == 1 and all(c.value is None for c in ws[1]):
                for i, h in enumerate(headers, start=1):
                    ws.cell(row=1, column=i, value=h)
                changed = True

    # Ensure Requisitions has new columns if missing
    ws_req = wb["Requisitions"]
    headers_req = [c.value for c in ws_req[1]]
    for col_name in ("category", "delivered", "status", "currency", "original_amount"):
        if col_name not in headers_req:
            ws_req.cell(row=1, column=len(headers_req) + 1, value=col_name)
            headers_req.append(col_name)
            changed = True

    # Ensure Landings has new columns if missing
    ws_land = wb["Landings"]
    headers_land = [c.value for c in ws_land[1]]
    for col_name in ("landed_date", "status", "delivered", "currency", "amount_original"):
        if col_name not in headers_land:
            ws_land.cell(row=1, column=len(headers_land) + 1, value=col_name)
            headers_land.append(col_name)
            changed = True

    if changed:
        wb.save(XLSX_PATH)

def _ensure_workbook():
    if XLSX_PATH.exists():
        wb = load_workbook(XLSX_PATH)
        _upgrade_schema(wb)
        wb.save(XLSX_PATH)
        return
    # Create new workbook from scratch
    wb = Workbook()
    # First sheet: Users
    ws = wb.active
    ws.title = "Users"
    ws.append(SHEETS["Users"])
    # seed admin user
    ws.append(["admin", generate_password_hash("admin123"), "admin"])

    # Other sheets
    for title in ["Directory", "Requisitions", "Landings", "Categories"]:
        ws2 = wb.create_sheet(title)
        ws2.append(SHEETS[title])

    _upgrade_schema(wb)  # ensure final schema
    wb.save(XLSX_PATH)

def _open_wb():
    _ensure_workbook()
    return load_workbook(XLSX_PATH)

def _sheet_rows(ws):
    headers = [c.value for c in ws[1]]
    out = []
    for r in ws.iter_rows(min_row=2, values_only=False):
        vals = [c.value for c in r]
        if all(v is None for v in vals):
            continue
        out.append(dict(zip(headers, vals)))
    return out

def _next_id(rows):
    ids = [int(r.get("id") or 0) for r in rows if str(r.get("id") or "").isdigit()]
    return (max(ids) + 1) if ids else 1

def _find_row_index_by(ws, key, value):
    headers = [c.value for c in ws[1]]
    try:
        col = headers.index(key) + 1
    except ValueError:
        return None
    for i in range(2, ws.max_row + 1):
        if ws.cell(row=i, column=col).value == value:
            return i
    return None

def _save_wb(wb):
    wb.save(XLSX_PATH)

# ---- Flask 3.x compatible init (runs once)
@app.before_request
def _init_guard():
    if not hasattr(app, "_init_done"):
        _ensure_workbook()
        app._init_done = True

def require_login():
    if "user" not in session:
        return jsonify({"error": "login_required"}), 401

def require_admin():
    if "user" not in session or session["user"]["role"] != "admin":
        return jsonify({"error": "admin_required"}), 403

# ===== FX API for frontend =====
@app.get("/api/currencies")
def currencies():
    rates = _get_fx_rates()

    # Default currencies you want to see in the dropdown
    default_codes = [
        "USD", "EUR", "AED", "GBP", "LBP",
        "SAR", "QAR", "KWD", "OMR", "CHF"
    ]

    # Even if API fails and only USD exists, we still show these codes
    codes = sorted(set(list(rates.keys()) + default_codes))

    return jsonify({"currencies": codes})

# ===== Auth =====
@app.post("/api/login")
def login():
    data = request.json or {}
    u = (data.get("username") or "").strip()
    p = (data.get("password") or "")
    with WB_LOCK:
        wb = _open_wb()
        ws = wb["Users"]
        users = _sheet_rows(ws)
    user = next((x for x in users if x["username"] == u), None)
    if not user or not check_password_hash(user["password_hash"], p):
        return jsonify({"error": "invalid_credentials"}), 400
    session["user"] = {"username": user["username"], "role": user["role"]}
    return jsonify(session["user"])

@app.post("/api/logout")
def logout():
    session.pop("user", None)
    return jsonify({"ok": True})

@app.get("/api/session")
def whoami():
    return jsonify(session.get("user"))

# ===== Users (admin) =====
@app.get("/api/users")
def users_list():
    need = require_admin()
    if need: return need
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Users"]; users = _sheet_rows(ws)
    return jsonify([{"username": u["username"], "role": u["role"]} for u in users])

@app.post("/api/users")
def users_add_update():
    need = require_admin()
    if need: return need
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "")
    role = data.get("role") or "user"
    if not username or not password or role not in ("admin","user"):
        return jsonify({"error": "bad_request"}), 400
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Users"]
        idx = _find_row_index_by(ws, "username", username)
        if idx:
            ws.cell(row=idx, column=2, value=generate_password_hash(password))
            ws.cell(row=idx, column=3, value=role)
        else:
            ws.append([username, generate_password_hash(password), role])
        _save_wb(wb)
    return jsonify({"ok": True})

@app.delete("/api/users/<username>")
def users_delete(username):
    need = require_admin()
    if need: return need
    if username == "admin":
        return jsonify({"error":"cannot_delete_admin"}), 400
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Users"]
        idx = _find_row_index_by(ws, "username", username)
        if idx:
            ws.delete_rows(idx); _save_wb(wb)
    return jsonify({"ok": True})

# ===== Directory =====
@app.get("/api/directory")
def dir_list():
    t = request.args.get("type")
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Directory"]; rows = _sheet_rows(ws)
    if t in ("supplier","workshop"):
        rows = [r for r in rows if r["type"] == t]
    for r in rows:
        r["id"] = int(r["id"])
        r["email"] = r.get("email") or ""
        r["phone"] = r.get("phone") or ""
        r["address"] = r.get("address") or ""
    return jsonify(rows)

@app.post("/api/directory")
def dir_add():
    need = require_login()
    if need: return need
    d = request.json or {}
    typ = d.get("type")
    name = (d.get("name") or "").strip()
    email = (d.get("email") or "").strip()
    phone = (d.get("phone") or "").strip()
    address = (d.get("address") or "").strip()
    if typ not in ("supplier","workshop") or not name:
        return jsonify({"error":"name_required"}), 400
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Directory"]; rows = _sheet_rows(ws)
        new_id = _next_id(rows)
        ws.append([new_id, typ, name, email, phone, address])
        _save_wb(wb)
    return jsonify({"ok": True})

@app.post("/api/directory/quick")
def dir_quick_add():
    need = require_login()
    if need: return need
    d = request.json or {}
    typ = d.get("type")
    name = (d.get("name") or "").strip()
    email = (d.get("email") or "").strip()
    phone = (d.get("phone") or "").strip()
    address = (d.get("address") or "").strip()
    if typ not in ("supplier","workshop") or not name:
        return jsonify({"error":"bad_request"}), 400
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Directory"]; rows = _sheet_rows(ws)
        new_id = _next_id(rows)
        ws.append([new_id, typ, name, email, phone, address])
        _save_wb(wb)
    return jsonify({"ok": True})

@app.patch("/api/directory/<int:did>")
def dir_update(did):
    need = require_login()
    if need: return need
    patch = request.json or {}
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Directory"]
        idx = _find_row_index_by(ws, "id", did)
        if not idx: return jsonify({"error":"not_found"}), 404
        headers = [c.value for c in ws[1]]
        for key in ("email","phone","address","name"):
            if key in patch and key in headers:
                col = headers.index(key) + 1
                ws.cell(row=idx, column=col, value=patch[key] or "")
        _save_wb(wb)
    return jsonify({"ok": True})

@app.delete("/api/directory/<int:did>")
def dir_delete(did):
    need = require_login()
    if need: return need
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Directory"]
        idx = _find_row_index_by(ws, "id", did)
        if idx:
            ws.delete_rows(idx); _save_wb(wb)
    return jsonify({"ok": True})

# ===== Categories (admin) =====
@app.get("/api/categories")
def cat_list():
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Categories"]; rows = _sheet_rows(ws)
    out = []
    for r in rows:
        r["id"] = int(r.get("id") or 0)
        r["name"] = r.get("name") or ""
        r["abbr"] = (r.get("abbr") or "").strip()
        out.append(r)
    return jsonify(out)

@app.post("/api/categories")
def cat_add():
    need = require_admin()
    if need: return need
    d = request.json or {}
    name = (d.get("name") or "").strip()
    abbr = (d.get("abbr") or "").strip()
    if not name or not abbr:
        return jsonify({"error":"bad_request"}), 400
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Categories"]; rows = _sheet_rows(ws)
        # unique abbreviation
        al = abbr.lower()
        if any((r.get("abbr") or "").strip().lower() == al for r in rows):
            return jsonify({"error":"duplicate_abbr"}), 400
        new_id = _next_id(rows)
        ws.append([new_id, name, abbr])
        _save_wb(wb)
    return jsonify({"ok": True})

@app.patch("/api/categories/<int:cid>")
def cat_update(cid):
    need = require_admin()
    if need: return need
    d = request.json or {}
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Categories"]; rows = _sheet_rows(ws)
        idx = _find_row_index_by(ws, "id", cid)
        if not idx: return jsonify({"error":"not_found"}), 404
        headers = [c.value for c in ws[1]]

        # check uniqueness if abbr changed
        new_abbr = d.get("abbr")
        if new_abbr is not None:
            new_abbr = new_abbr.strip()
            al = new_abbr.lower()
            for r in rows:
                if int(r.get("id") or 0) == cid:
                    continue
                if (r.get("abbr") or "").strip().lower() == al:
                    return jsonify({"error":"duplicate_abbr"}), 400
            ws.cell(row=idx, column=headers.index("abbr")+1, value=new_abbr)

        if "name" in d:
            ws.cell(row=idx, column=headers.index("name")+1, value=d["name"] or "")
        _save_wb(wb)
    return jsonify({"ok": True})

@app.delete("/api/categories/<int:cid>")
def cat_delete(cid):
    need = require_admin()
    if need: return need
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Categories"]
        idx = _find_row_index_by(ws, "id", cid)
        if idx:
            ws.delete_rows(idx); _save_wb(wb)
    return jsonify({"ok": True})

# ===== Requisitions =====
@app.get("/api/requisitions")
def req_list():
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Requisitions"]; rows = _sheet_rows(ws)
    for r in rows:
        r["id"] = int(r["id"])
        r["total_amount"] = float(r.get("total_amount") or 0.0)
        r["paid"] = int(r.get("paid") or 0)
        r["category"] = (r.get("category") or "").strip()
        r["delivered"] = int(r.get("delivered") or 0)
        r["status"] = (r.get("status") or "open").strip().lower()
        r["currency"] = (r.get("currency") or "USD").upper()
        if r.get("original_amount") is None:
            r["original_amount"] = r["total_amount"]
        else:
            r["original_amount"] = float(r.get("original_amount") or 0.0)
    return jsonify(sorted(rows, key=lambda x: x["id"], reverse=True))

@app.post("/api/requisitions")
def req_add():
    need = require_login()
    if need: return need
    d = request.json or {}
    amount_raw = d.get("amount", d.get("total_amount"))
    required_base = ["number","vessel","supplier","date_ordered","category"]
    if any(not d.get(k) for k in required_base) or amount_raw in (None, "", []):
        return jsonify({"error":"missing_fields"}), 400

    category_abbr = (d.get("category") or "").strip()
    currency = (d.get("currency") or "USD").upper()

    with WB_LOCK:
        wb = _open_wb()
        ws_req = wb["Requisitions"]; rows = _sheet_rows(ws_req)

        num_lower = d["number"].strip().lower()
        if any((r.get("number") or "").strip().lower() == num_lower for r in rows):
            return jsonify({"error":"duplicate_number"}), 400

        # validate category abbreviation
        ws_cat = wb["Categories"]
        cat_rows = _sheet_rows(ws_cat)
        abbr_lower = category_abbr.lower()
        if not any((r.get("abbr") or "").strip().lower() == abbr_lower for r in cat_rows):
            return jsonify({"error": "unknown_category"}), 400

        original_amount = float(amount_raw)
        usd_amount = convert_to_usd(original_amount, currency)

        new_id = _next_id(rows)
        ws_req.append([
            new_id,
            d["number"].strip(),
            d["vessel"].strip(),
            d["supplier"],
            d["date_ordered"],
            d.get("expected") or "",
            usd_amount,                     # stored in USD
            1 if d.get("paid") else 0,
            category_abbr,
            0,                              # delivered
            "open",                         # status
            currency,
            original_amount,
        ])
        _save_wb(wb)
    return jsonify({"ok": True})

@app.patch("/api/requisitions/<int:rid>/toggle_paid")
def req_toggle_paid(rid):
    need = require_login()
    if need: return need
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Requisitions"]
        idx = _find_row_index_by(ws, "id", rid)
        if not idx: return jsonify({"error":"not_found"}), 404
        headers = [c.value for c in ws[1]]
        col = headers.index("paid") + 1
        cur = int(ws.cell(row=idx, column=col).value or 0)
        ws.cell(row=idx, column=col, value=0 if cur else 1)
        _save_wb(wb)
    return jsonify({"ok": True})

@app.patch("/api/requisitions/<int:rid>")
def req_update(rid):
    need = require_login()
    if need: return need
    patch = request.json or {}
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Requisitions"]; rows = _sheet_rows(ws)
        idx = _find_row_index_by(ws, "id", rid)
        if not idx: return jsonify({"error":"not_found"}), 404
        headers = [c.value for c in ws[1]]

        cur = next((r for r in rows if int(r["id"]) == rid), None)

        # uniqueness for number
        if "number" in patch:
            new_num = (patch["number"] or "").strip()
            if not new_num:
                return jsonify({"error":"number_required"}), 400
            for r in rows:
                if int(r["id"]) == rid:
                    continue
                if (r.get("number") or "").strip().lower() == new_num.lower():
                    return jsonify({"error":"duplicate_number"}), 400
            ws.cell(row=idx, column=headers.index("number")+1, value=new_num)

        # category validation if changed
        if "category" in patch:
            category_abbr = (patch["category"] or "").strip()
            ws_cat = wb["Categories"]
            cat_rows = _sheet_rows(ws_cat)
            abbr_lower = category_abbr.lower()
            if not any((r.get("abbr") or "").strip().lower() == abbr_lower for r in cat_rows):
                return jsonify({"error": "unknown_category"}), 400
            ws.cell(row=idx, column=headers.index("category")+1, value=category_abbr)

        # amount + currency
        if any(k in patch for k in ("amount", "total_amount", "currency")):
            cur_currency = (cur.get("currency") or "USD").upper() if cur else "USD"
            cur_orig = float(cur.get("original_amount") or cur.get("total_amount") or 0.0) if cur else 0.0
            amount_raw = patch.get("amount", patch.get("total_amount", cur_orig))
            currency = (patch.get("currency") or cur_currency).upper()

            original_amount = float(amount_raw)
            usd_amount = convert_to_usd(original_amount, currency)

            ws.cell(row=idx, column=headers.index("total_amount")+1, value=usd_amount)
            if "currency" in headers:
                ws.cell(row=idx, column=headers.index("currency")+1, value=currency)
            if "original_amount" in headers:
                ws.cell(row=idx, column=headers.index("original_amount")+1, value=original_amount)

        # numeric toggles
        if "paid" in patch:
            ws.cell(row=idx, column=headers.index("paid")+1,
                    value=1 if patch["paid"] else 0)
        if "delivered" in patch:
            ws.cell(row=idx, column=headers.index("delivered")+1,
                    value=1 if patch["delivered"] else 0)

        # status
        if "status" in patch:
            st = (patch["status"] or "").strip().lower()
            if st not in ("open","cancelled"):
                return jsonify({"error":"bad_status"}), 400
            ws.cell(row=idx, column=headers.index("status")+1, value=st)

        # other fields
        for key in ("vessel","supplier","date_ordered","expected"):
            if key in patch and key in headers:
                ws.cell(row=idx, column=headers.index(key)+1, value=patch[key] or "")

        _save_wb(wb)
    return jsonify({"ok": True})

@app.delete("/api/requisitions/<int:rid>")
def req_delete(rid):
    need = require_login()
    if need: return need
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Requisitions"]
        idx = _find_row_index_by(ws, "id", rid)
        if idx:
            ws.delete_rows(idx); _save_wb(wb)
    return jsonify({"ok": True})

# ===== Landings =====
@app.get("/api/landings")
def land_list():
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Landings"]; rows = _sheet_rows(ws)
    for r in rows:
        r["id"] = int(r["id"])
        r["amount"] = float(r.get("amount") or 0.0)
        r["paid"] = int(r.get("paid") or 0)
        r["expected"] = r.get("expected") or ""
        r["landed_date"] = r.get("landed_date") or ""
        r["status"] = (r.get("status") or "open").strip().lower()
        r["delivered"] = int(r.get("delivered") or 0)
        r["currency"] = (r.get("currency") or "USD").upper()
        if r.get("amount_original") is None:
            r["amount_original"] = r["amount"]
        else:
            r["amount_original"] = float(r.get("amount_original") or 0.0)
    return jsonify(sorted(rows, key=lambda x: x["id"], reverse=True))

@app.post("/api/landings")
def land_add():
    need = require_login()
    if need: return need
    d = request.json or {}
    amount_raw = d.get("amount")
    required = ["vessel","item","workshop"]
    if any(not d.get(k) for k in required) or amount_raw in (None, "", []):
        return jsonify({"error":"missing_fields"}), 400

    currency = (d.get("currency") or "USD").upper()
    original_amount = float(amount_raw)
    usd_amount = convert_to_usd(original_amount, currency)

    with WB_LOCK:
        wb = _open_wb(); ws = wb["Landings"]; rows = _sheet_rows(ws)
        new_id = _next_id(rows)
        ws.append([
            new_id,
            d["vessel"].strip(),
            d["item"].strip(),
            d["workshop"],
            usd_amount,                    # stored in USD
            1 if d.get("paid") else 0,
            d.get("expected") or "",
            d.get("landed_date") or "",
            "open",
            0,                             # delivered
            currency,
            original_amount,
        ])
        _save_wb(wb)
    return jsonify({"ok": True})

@app.patch("/api/landings/<int:lid>/toggle_paid")
def land_toggle_paid(lid):
    need = require_login()
    if need: return need
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Landings"]
        idx = _find_row_index_by(ws, "id", lid)
        if not idx: return jsonify({"error":"not_found"}), 404
        headers = [c.value for c in ws[1]]
        col = headers.index("paid") + 1
        cur = int(ws.cell(row=idx, column=col).value or 0)
        ws.cell(row=idx, column=col, value=0 if cur else 1)
        _save_wb(wb)
    return jsonify({"ok": True})

@app.patch("/api/landings/<int:lid>")
def land_update(lid):
    need = require_login()
    if need: return need
    patch = request.json or {}
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Landings"]; rows = _sheet_rows(ws)
        idx = _find_row_index_by(ws, "id", lid)
        if not idx: return jsonify({"error":"not_found"}), 404
        headers = [c.value for c in ws[1]]

        cur = next((r for r in rows if int(r["id"]) == lid), None)

        # amount + currency
        if any(k in patch for k in ("amount", "currency")):
            cur_currency = (cur.get("currency") or "USD").upper() if cur else "USD"
            cur_orig = float(cur.get("amount_original") or cur.get("amount") or 0.0) if cur else 0.0
            amount_raw = patch.get("amount", cur_orig)
            currency = (patch.get("currency") or cur_currency).upper()

            original_amount = float(amount_raw)
            usd_amount = convert_to_usd(original_amount, currency)

            ws.cell(row=idx, column=headers.index("amount")+1, value=usd_amount)
            if "currency" in headers:
                ws.cell(row=idx, column=headers.index("currency")+1, value=currency)
            if "amount_original" in headers:
                ws.cell(row=idx, column=headers.index("amount_original")+1, value=original_amount)

        if "paid" in patch:
            ws.cell(row=idx, column=headers.index("paid")+1,
                    value=1 if patch["paid"] else 0)
        if "delivered" in patch:
            ws.cell(row=idx, column=headers.index("delivered")+1,
                    value=1 if patch["delivered"] else 0)

        if "status" in patch:
            st = (patch["status"] or "").strip().lower()
            if st not in ("open","cancelled"):
                return jsonify({"error":"bad_status"}), 400
            ws.cell(row=idx, column=headers.index("status")+1, value=st)

        for key in ("vessel","item","workshop","expected","landed_date"):
            if key in patch and key in headers:
                ws.cell(row=idx, column=headers.index(key)+1, value=patch[key] or "")

        _save_wb(wb)
    return jsonify({"ok": True})

@app.delete("/api/landings/<int:lid>")
def land_delete(lid):
    need = require_login()
    if need: return need
    with WB_LOCK:
        wb = _open_wb(); ws = wb["Landings"]
        idx = _find_row_index_by(ws, "id", lid)
        if idx:
            ws.delete_rows(idx); _save_wb(wb)
    return jsonify({"ok": True})

# ===== Serve SPA =====
@app.get("/")
def root():
    return send_from_directory("static", "index.html")

if __name__ == "__main__":
    _ensure_workbook()
    app.run(host="0.0.0.0", port=8000, debug=True)


