from flask import Flask, request, jsonify, session, send_from_directory
from flask_cors import CORS
import pandas as pd
import os, json, time
import requests
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = os.environ.get("APP_SECRET", "amt-procurement-secret")
CORS(app, supports_credentials=True)

DATA_FILE = os.environ.get("DATA_FILE", "data.xlsx")

SHEETS = {
    "Requisitions": [
        "id", "number", "vessel", "category", "supplier",
        "date_ordered", "expected",
        "original_amount", "currency", "total_amount",  # total_amount in USD
        "paid", "delivered", "status"
    ],
    "Landings": [
        "id", "vessel", "item", "workshop",
        "expected", "landed_date",
        "amount_original", "currency", "amount",         # amount in USD
        "paid", "delivered", "status"
    ],
    "Directory": ["id", "type", "name", "email", "phone", "address"],
    "Categories": ["id", "name", "abbr"],
    "Users": ["username", "password_hash", "role"],
    "Vessels": ["id", "name"],
}

# ---------- Excel helpers ----------
def _open_wb():
    if not os.path.exists(DATA_FILE):
        with pd.ExcelWriter(DATA_FILE, engine="openpyxl") as w:
            for s, cols in SHEETS.items():
                pd.DataFrame(columns=cols).to_excel(w, s, index=False)

def _upgrade_schema():
    _open_wb()
    wb = pd.ExcelFile(DATA_FILE, engine="openpyxl")
    with pd.ExcelWriter(DATA_FILE, engine="openpyxl", mode="a", if_sheet_exists="overlay") as w:
        for sheet, cols in SHEETS.items():
            if sheet not in wb.sheet_names:
                pd.DataFrame(columns=cols).to_excel(w, sheet, index=False)
                continue
            df = pd.read_excel(DATA_FILE, sheet_name=sheet)
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]
            df.to_excel(w, sheet, index=False)

def read_sheet(sheet):
    _upgrade_schema()
    try:
        df = pd.read_excel(DATA_FILE, sheet_name=sheet)
    except Exception:
        df = pd.DataFrame(columns=SHEETS[sheet])
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")

def write_sheet(sheet, rows):
    _upgrade_schema()
    df = pd.DataFrame(rows, columns=SHEETS[sheet])
    with pd.ExcelWriter(DATA_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet, index=False)

def next_id(rows):
    if not rows:
        return 1
    return max(int(r.get("id") or 0) for r in rows) + 1

# ---------- Auth helpers ----------
def require_login():
    if not session.get("username"):
        return jsonify({"error": "login_required"}), 401
    return None

def require_admin():
    if not session.get("username"):
        return jsonify({"error": "login_required"}), 401
    if session.get("role") != "admin":
        return jsonify({"error": "admin_required"}), 403
    return None

# ---------- FX helpers ----------
FX_CACHE = {"ts": 0, "rates": {"USD": 1.0}}
FX_TTL = 60 * 60  # 1 hour

def get_rates():
    now = time.time()
    if now - FX_CACHE["ts"] < FX_TTL and FX_CACHE.get("rates"):
        return FX_CACHE["rates"]

    try:
        r = requests.get(
            "https://open.er-api.com/v6/latest/USD",
            timeout=8
        )
        data = r.json()
        rates = data.get("rates") or {"USD": 1.0}
        rates["USD"] = 1.0
        FX_CACHE["rates"] = rates
        FX_CACHE["ts"] = now
        return rates
    except Exception:
        return FX_CACHE.get("rates") or {"USD": 1.0}

def to_usd(amount, currency):
    if amount is None:
        return 0.0
    try:
        amount = float(amount)
    except Exception:
        return 0.0
    currency = (currency or "USD").upper()
    rates = get_rates()
    rate = rates.get(currency)
    if not rate:
        return amount
    return amount / rate

# ---------- Static ----------
@app.get("/")
def index():
    return send_from_directory("templates", "index.html")

@app.get("/static/<path:p>")
def static_files(p):
    return send_from_directory("static", p)

# ---------- Session ----------
@app.get("/api/session")
def api_session():
    if session.get("username"):
        return jsonify({"username": session["username"], "role": session.get("role", "user")})
    return jsonify({})

@app.post("/api/login")
def api_login():
    data = request.get_json(force=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    users = read_sheet("Users")
    u = next((x for x in users if (x.get("username") or "").lower() == username.lower()), None)
    if not u or not check_password_hash(u.get("password_hash") or "", password):
        return jsonify({"error": "invalid_credentials"}), 401

    session["username"] = u["username"]
    session["role"] = u.get("role", "user")
    return jsonify({"ok": True})

@app.post("/api/logout")
def api_logout():
    session.clear()
    return jsonify({"ok": True})

# ---------- Currencies ----------
@app.get("/api/currencies")
def api_currencies():
    rates = get_rates()
    return jsonify({"currencies": sorted(list(rates.keys()))})

# ===== Vessels (admin-managed master list) =====
@app.get("/api/vessels")
def vessels_list():
    rows = read_sheet("Vessels")
    rows = sorted(rows, key=lambda r: (r.get("name") or "").lower())
    return jsonify(rows)

@app.post("/api/vessels")
def vessels_add():
    err = require_admin()
    if err: return err
    data = request.get_json(force=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name_required"}), 400

    rows = read_sheet("Vessels")
    if any((r.get("name") or "").strip().lower() == name.lower() for r in rows):
        return jsonify({"error": "duplicate_name"}), 400

    new_id = next_id(rows)
    rows.append({"id": new_id, "name": name})
    write_sheet("Vessels", rows)
    return jsonify({"id": new_id, "name": name})

@app.patch("/api/vessels/<int:vid>")
def vessels_edit(vid):
    err = require_admin()
    if err: return err
    data = request.get_json(force=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name_required"}), 400

    rows = read_sheet("Vessels")
    if any((r.get("name") or "").strip().lower() == name.lower() and int(r.get("id") or 0) != vid for r in rows):
        return jsonify({"error": "duplicate_name"}), 400

    found = False
    for r in rows:
        if int(r.get("id") or 0) == vid:
            r["name"] = name
            found = True
            break
    if not found:
        return jsonify({"error": "not_found"}), 404

    write_sheet("Vessels", rows)
    return jsonify({"ok": True})

@app.delete("/api/vessels/<int:vid>")
def vessels_delete(vid):
    err = require_admin()
    if err: return err
    rows = read_sheet("Vessels")
    new_rows = [r for r in rows if int(r.get("id") or 0) != vid]
    if len(new_rows) == len(rows):
        return jsonify({"error": "not_found"}), 404
    write_sheet("Vessels", new_rows)
    return jsonify({"ok": True})

# ===== Categories (admin) =====
@app.get("/api/categories")
def cat_list():
    rows = read_sheet("Categories")
    return jsonify(rows)

@app.post("/api/categories")
def cat_add():
    err = require_admin()
    if err: return err
    data = request.get_json(force=True) or {}
    name = (data.get("name") or "").strip()
    abbr = (data.get("abbr") or "").strip()
    if not name or not abbr:
        return jsonify({"error": "bad_request"}), 400

    rows = read_sheet("Categories")
    if any((r.get("abbr") or "").lower() == abbr.lower() for r in rows):
        return jsonify({"error": "duplicate_abbr"}), 400

    new_id = next_id(rows)
    rows.append({"id": new_id, "name": name, "abbr": abbr})
    write_sheet("Categories", rows)
    return jsonify({"ok": True, "id": new_id})

@app.patch("/api/categories/<int:cid>")
def cat_edit(cid):
    err = require_admin()
    if err: return err
    data = request.get_json(force=True) or {}
    name = (data.get("name") or "").strip()
    abbr = (data.get("abbr") or "").strip()
    if not name or not abbr:
        return jsonify({"error": "bad_request"}), 400

    rows = read_sheet("Categories")
    if any((r.get("abbr") or "").lower() == abbr.lower() and int(r.get("id") or 0) != cid for r in rows):
        return jsonify({"error": "duplicate_abbr"}), 400

    for r in rows:
        if int(r.get("id") or 0) == cid:
            r["name"] = name
            r["abbr"] = abbr
            break
    write_sheet("Categories", rows)
    return jsonify({"ok": True})

@app.delete("/api/categories/<int:cid>")
def cat_delete(cid):
    err = require_admin()
    if err: return err
    rows = read_sheet("Categories")
    rows = [r for r in rows if int(r.get("id") or 0) != cid]
    write_sheet("Categories", rows)
    return jsonify({"ok": True})

# ===== Directory =====
@app.get("/api/directory")
def dir_list():
    t = request.args.get("type")
    rows = read_sheet("Directory")
    if t:
        rows = [r for r in rows if (r.get("type") or "").lower() == t.lower()]
    return jsonify(rows)

@app.post("/api/directory/quick")
def dir_add_quick():
    err = require_login()
    if err: return err
    data = request.get_json(force=True) or {}
    dtype = (data.get("type") or "").strip().lower()
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip()
    phone = (data.get("phone") or "").strip()
    address = (data.get("address") or "").strip()

    if dtype not in ("supplier", "workshop") or not name:
        return jsonify({"error": "bad_request"}), 400

    rows = read_sheet("Directory")
    new_id = next_id(rows)
    rows.append({
        "id": new_id, "type": dtype, "name": name,
        "email": email, "phone": phone, "address": address
    })
    write_sheet("Directory", rows)
    return jsonify({"ok": True, "id": new_id})

# ===== Users (admin) =====
@app.get("/api/users")
def users_list():
    err = require_admin()
    if err: return err
    users = read_sheet("Users")
    return jsonify([{"username": u["username"], "role": u.get("role", "user")} for u in users])

@app.post("/api/users")
def users_add():
    err = require_admin()
    if err: return err
    data = request.get_json(force=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = (data.get("role") or "user").strip()

    if not username or not password:
        return jsonify({"error": "bad_request"}), 400

    rows = read_sheet("Users")
    if any((r.get("username") or "").lower() == username.lower() for r in rows):
        return jsonify({"error": "duplicate_user"}), 400

    rows.append({
        "username": username,
        "password_hash": generate_password_hash(password),
        "role": role if role in ("admin", "user") else "user"
    })
    write_sheet("Users", rows)
    return jsonify({"ok": True})

@app.delete("/api/users/<username>")
def users_delete(username):
    err = require_admin()
    if err: return err
    rows = read_sheet("Users")
    rows = [r for r in rows if (r.get("username") or "").lower() != username.lower()]
    write_sheet("Users", rows)
    return jsonify({"ok": True})

# ===== Requisitions =====
@app.get("/api/requisitions")
def req_list():
    rows = read_sheet("Requisitions")
    return jsonify(rows)

@app.post("/api/requisitions")
def req_add():
    err = require_login()
    if err: return err
    data = request.get_json(force=True) or {}
    number = (data.get("number") or "").strip()
    vessel = (data.get("vessel") or "").strip()
    category = (data.get("category") or "").strip()
    supplier = (data.get("supplier") or "").strip()
    date_ordered = data.get("date_ordered")
    expected = data.get("expected")
    amount = data.get("amount")
    currency = (data.get("currency") or "USD").upper()
    paid = 1 if data.get("paid") else 0

    if not number:
        return jsonify({"error": "bad_request"}), 400

    rows = read_sheet("Requisitions")
    if any((r.get("number") or "").lower() == number.lower() for r in rows):
        return jsonify({"error": "duplicate_number"}), 400

    usd = to_usd(amount, currency)

    new_id = next_id(rows)
    rows.append({
        "id": new_id,
        "number": number,
        "vessel": vessel,
        "category": category,
        "supplier": supplier,
        "date_ordered": date_ordered,
        "expected": expected,
        "original_amount": float(amount),
        "currency": currency,
        "total_amount": float(usd),
        "paid": paid,
        "delivered": 0,
        "status": "open"
    })
    write_sheet("Requisitions", rows)
    return jsonify({"ok": True, "id": new_id})

@app.patch("/api/requisitions/<int:rid>")
def req_edit(rid):
    err = require_login()
    if err: return err
    data = request.get_json(force=True) or {}

    rows = read_sheet("Requisitions")
    row = next((r for r in rows if int(r.get("id") or 0) == rid), None)
    if not row:
        return jsonify({"error": "not_found"}), 404

    # update allowed fields
    for k in ["number","vessel","category","supplier","date_ordered","expected","status"]:
        if k in data:
            row[k] = data[k]

    if "paid" in data:
        row["paid"] = 1 if data["paid"] else 0
    if "delivered" in data:
        row["delivered"] = 1 if data["delivered"] else 0

    if "amount" in data or "currency" in data:
        amount = data.get("amount", row.get("original_amount"))
        currency = (data.get("currency", row.get("currency")) or "USD").upper()
        row["original_amount"] = float(amount)
        row["currency"] = currency
        row["total_amount"] = float(to_usd(amount, currency))

    write_sheet("Requisitions", rows)
    return jsonify({"ok": True})

@app.patch("/api/requisitions/<int:rid>/toggle_paid")
def req_toggle_paid(rid):
    err = require_login()
    if err: return err
    rows = read_sheet("Requisitions")
    for r in rows:
        if int(r.get("id") or 0) == rid:
            r["paid"] = 0 if r.get("paid") else 1
            break
    write_sheet("Requisitions", rows)
    return jsonify({"ok": True})

@app.delete("/api/requisitions/<int:rid>")
def req_delete(rid):
    err = require_login()
    if err: return err
    rows = read_sheet("Requisitions")
    rows = [r for r in rows if int(r.get("id") or 0) != rid]
    write_sheet("Requisitions", rows)
    return jsonify({"ok": True})

# ===== Landings =====
@app.get("/api/landings")
def land_list():
    rows = read_sheet("Landings")
    return jsonify(rows)

@app.post("/api/landings")
def land_add():
    err = require_login()
    if err: return err
    data = request.get_json(force=True) or {}
    vessel = (data.get("vessel") or "").strip()
    item = (data.get("item") or "").strip()
    workshop = (data.get("workshop") or "").strip()
    expected = data.get("expected")
    landed_date = data.get("landed_date")
    amount = data.get("amount")
    currency = (data.get("currency") or "USD").upper()
    paid = 1 if data.get("paid") else 0

    rows = read_sheet("Landings")
    new_id = next_id(rows)
    usd = to_usd(amount, currency)

    rows.append({
        "id": new_id,
        "vessel": vessel,
        "item": item,
        "workshop": workshop,
        "expected": expected,
        "landed_date": landed_date,
        "amount_original": float(amount),
        "currency": currency,
        "amount": float(usd),
        "paid": paid,
        "delivered": 0,
        "status": "open"
    })
    write_sheet("Landings", rows)
    return jsonify({"ok": True, "id": new_id})

@app.patch("/api/landings/<int:lid>")
def land_edit(lid):
    err = require_login()
    if err: return err
    data = request.get_json(force=True) or {}

    rows = read_sheet("Landings")
    row = next((r for r in rows if int(r.get("id") or 0) == lid), None)
    if not row:
        return jsonify({"error": "not_found"}), 404

    for k in ["vessel","item","workshop","expected","landed_date","status"]:
        if k in data:
            row[k] = data[k]

    if "paid" in data:
        row["paid"] = 1 if data["paid"] else 0
    if "delivered" in data:
        row["delivered"] = 1 if data["delivered"] else 0

    if "amount" in data or "currency" in data:
        amount = data.get("amount", row.get("amount_original"))
        currency = (data.get("currency", row.get("currency")) or "USD").upper()
        row["amount_original"] = float(amount)
        row["currency"] = currency
        row["amount"] = float(to_usd(amount, currency))

    write_sheet("Landings", rows)
    return jsonify({"ok": True})

@app.patch("/api/landings/<int:lid>/toggle_paid")
def land_toggle_paid(lid):
    err = require_login()
    if err: return err
    rows = read_sheet("Landings")
    for r in rows:
        if int(r.get("id") or 0) == lid:
            r["paid"] = 0 if r.get("paid") else 1
            break
    write_sheet("Landings", rows)
    return jsonify({"ok": True})

@app.delete("/api/landings/<int:lid>")
def land_delete(lid):
    err = require_login()
    if err: return err
    rows = read_sheet("Landings")
    rows = [r for r in rows if int(r.get("id") or 0) != lid]
    write_sheet("Landings", rows)
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(debug=True)
