# ============================================
# AMT PROCUREMENT - FINAL BACKEND (PART 1/6)
# Excel-based Database + Backups + Authentication
# ============================================


import os
import json
import hashlib
import requests
from datetime import datetime
import openpyxl

from flask import Flask, jsonify, request, session, send_file
from flask_cors import CORS

# --------------------------------------------
# App INIT
# --------------------------------------------
app = Flask(__name__)
app.secret_key = "SECRET_KEY_987654321"
CORS(app, supports_credentials=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_FILE = os.path.join(BASE_DIR, "office_ops.xlsx")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")

if not os.path.exists(BACKUP_DIR):
    os.makedirs(BACKUP_DIR)


# --------------------------------------------
# Helpers
# --------------------------------------------

def load_wb():
    """Load workbook."""
    return openpyxl.load_workbook(MAIN_FILE)

def save_wb(wb):
    """Save workbook to main file."""
    wb.save(MAIN_FILE)

def make_backup():
    """Create automatic timestamped backup."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"backup_{timestamp}.xlsx")
    wb = load_wb()
    wb.save(backup_path)
    return backup_path

def list_backups():
    """Return sorted backup list."""
    files = [f for f in os.listdir(BACKUP_DIR) if f.endswith(".xlsx")]
    return sorted(files, reverse=True)

def restore_backup(filename):
    """Overwrite main Excel with selected backup."""
    src = os.path.join(BACKUP_DIR, filename)
    if not os.path.exists(src):
        return False
    os.replace(src, MAIN_FILE)
    return True

def delete_backup(filename):
    """Delete a backup permanently."""
    path = os.path.join(BACKUP_DIR, filename)
    if not os.path.exists(path):
        return False
    os.remove(path)
    return True


# --------------------------------------------
# Hash password
# --------------------------------------------
def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


# --------------------------------------------
# Session helpers
# --------------------------------------------
def require_login():
    return "username" in session

def require_admin():
    return require_login() and session.get("role") == "admin"


# ============================================
# PART 2 — LOGIN + USERS API
# ============================================

@app.route("/api/login", methods=["POST"])
def login():
    data = request.json
    username = data.get("username", "").strip()
    password = data.get("password", "")

    wb = load_wb()
    ws = wb["Users"]

    for row in ws.iter_rows(min_row=2, values_only=True):
        u, pw_hash, role = row
        if u == username and pw_hash == hash_pw(password):
            session["username"] = username
            session["role"] = role
            return jsonify({"success": True})

    return jsonify({"error": "invalid_credentials"}), 400


@app.route("/api/session")
def session_info():
    if not require_login():
        return jsonify({})
    return jsonify({
        "username": session["username"],
        "role": session["role"]
    })


@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"success": True})


# -------------------------------
# LIST USERS (ADMIN ONLY)
# -------------------------------
@app.route("/api/users")
def list_users_api():
    if not require_admin():
        return jsonify({"error": "admin_required"}), 403

    wb = load_wb()
    ws = wb["Users"]

    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        username, pw_hash, role = row
        out.append({"username": username, "role": role})

    return jsonify(out)


# -------------------------------
# ADD USER
# -------------------------------
@app.route("/api/users", methods=["POST"])
def add_user():
    if not require_admin():
        return jsonify({"error": "admin_required"}), 403

    data = request.json
    username = data.get("username", "").strip()
    password = data.get("password", "")
    role = data.get("role", "user")

    if not username or not password:
        return jsonify({"error": "missing_fields"}), 400

    wb = load_wb()
    ws = wb["Users"]

    # Check duplicate
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0] == username:
            return jsonify({"error": "duplicate"}), 400

    # Insert
    ws.append([username, hash_pw(password), role])
    save_wb(wb)
    make_backup()

    return jsonify({"success": True})


# -------------------------------
# DELETE USER
# -------------------------------
@app.route("/api/users/<username>", methods=["DELETE"])
def delete_user(username):
    if not require_admin():
        return jsonify({"error": "admin_required"}), 403

    wb = load_wb()
    ws = wb["Users"]

    for i, row in enumerate(ws.iter_rows(min_row=2), start=2):
        if row[0].value == username:
            ws.delete_rows(i)
            save_wb(wb)
            make_backup()
            return jsonify({"success": True})

    return jsonify({"error": "not_found"}), 404


# ============================================
# PART 3 — DIRECTORY (SUPPLIERS + WORKSHOPS)
# ============================================

@app.route("/api/directory")
def get_directory():
    """
    Returns full directory, or filtered by type:
    /api/directory?type=supplier
    /api/directory?type=workshop
    """
    wb = load_wb()
    ws = wb["Directory"]

    filter_type = request.args.get("type")

    results = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        id_, type_, name, email, phone, address = row

        if filter_type and type_ != filter_type:
            continue

        results.append({
            "id": id_,
            "type": type_,
            "name": name,
            "email": email,
            "phone": phone,
            "address": address
        })

    return jsonify(results)


@app.route("/api/directory/quick", methods=["POST"])
def quick_add_directory():
    """
    Add new Supplier or Workshop from popup.
    """
    if not require_login():
        return jsonify({"error": "login_required"}), 403

    data = request.json
    type_ = data.get("type")
    name = data.get("name", "").strip()
    email = data.get("email", "")
    phone = data.get("phone", "")
    address = data.get("address", "")

    if not name or not type_:
        return jsonify({"error": "missing_fields"}), 400

    wb = load_wb()
    ws = wb["Directory"]

    # Auto ID = max existing ID + 1
    max_id = 0
    for r in ws.iter_rows(min_row=2, values_only=True):
        if r[0] and r[0] > max_id:
            max_id = r[0]

    new_id = max_id + 1

    ws.append([new_id, type_, name, email, phone, address])

    save_wb(wb)
    make_backup()

    return jsonify({"success": True})



# ============================================
# PART 4 — VESSELS + CATEGORIES
# ============================================

# ---------------------------
# LIST VESSELS
# ---------------------------
@app.route("/api/vessels")
def list_vessels():
    wb = load_wb()
    ws = wb["Vessels"]

    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        id_, name = row
        out.append({"id": id_, "name": name})

    return jsonify(out)


# ---------------------------
# ADD VESSEL
# ---------------------------
@app.route("/api/vessels", methods=["POST"])
def add_vessel():
    if not require_admin():
        return jsonify({"error": "admin_required"}), 403

    data = request.json
    name = data.get("name", "").strip()

    if not name:
        return jsonify({"error": "missing_name"}), 400

    wb = load_wb()
    ws = wb["Vessels"]

    # Prevent duplicates
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[1] == name:
            return jsonify({"error": "duplicate_name"}), 400

    max_id = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0] and row[0] > max_id:
            max_id = row[0]

    ws.append([max_id + 1, name])
    save_wb(wb)
    make_backup()

    return jsonify({"success": True})


# ---------------------------
# EDIT VESSEL
# ---------------------------
@app.route("/api/vessels/<int:id_>", methods=["PATCH"])
def edit_vessel(id_):
    if not require_admin():
        return jsonify({"error": "admin_required"}), 403

    data = request.json
    new_name = data.get("name", "").strip()

    if not new_name:
        return jsonify({"error": "missing_name"}), 400

    wb = load_wb()
    ws = wb["Vessels"]

    for row in ws.iter_rows(min_row=2):
        if row[0].value == id_:
            row[1].value = new_name
            save_wb(wb)
            make_backup()
            return jsonify({"success": True})

    return jsonify({"error": "not_found"}), 404


# ---------------------------
# DELETE VESSEL
# ---------------------------
@app.route("/api/vessels/<int:id_>", methods=["DELETE"])
def delete_vessel(id_):
    if not require_admin():
        return jsonify({"error": "admin_required"}), 403

    wb = load_wb()
    ws = wb["Vessels"]

    for i, row in enumerate(ws.iter_rows(min_row=2), start=2):
        if row[0].value == id_:
            ws.delete_rows(i)
            save_wb(wb)
            make_backup()
            return jsonify({"success": True})

    return jsonify({"error": "not_found"}), 404


# ============================================
# CATEGORIES
# ============================================

@app.route("/api/categories")
def list_categories():
    wb = load_wb()
    ws = wb["Categories"]

    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        id_, name, abbr = row
        out.append({"id": id_, "name": name, "abbr": abbr})

    return jsonify(out)


@app.route("/api/categories", methods=["POST"])
def add_category():
    if not require_admin():
        return jsonify({"error": "admin_required"}), 403

    data = request.json
    name = data.get("name", "").strip()
    abbr = data.get("abbr", "").strip()

    if not name or not abbr:
        return jsonify({"error": "missing_fields"}), 400

    wb = load_wb()
    ws = wb["Categories"]

    # Prevent abbreviation duplicates
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[2] == abbr:
            return jsonify({"error": "duplicate_abbr"}), 400

    max_id = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0] and row[0] > max_id:
            max_id = row[0]

    ws.append([max_id + 1, name, abbr])
    save_wb(wb)
    make_backup()

    return jsonify({"success": True})


@app.route("/api/categories/<int:id_>", methods=["PATCH"])
def edit_category(id_):
    if not require_admin():
        return jsonify({"error": "admin_required"}), 403

    data = request.json
    new_name = data.get("name", "").strip()
    new_abbr = data.get("abbr", "").strip()

    wb = load_wb()
    ws = wb["Categories"]

    for row in ws.iter_rows(min_row=2):
        if row[0].value == id_:
            row[1].value = new_name
            row[2].value = new_abbr
            save_wb(wb)
            make_backup()
            return jsonify({"success": True})

    return jsonify({"error": "not_found"}), 404


@app.route("/api/categories/<int:id_>", methods=["DELETE"])
def delete_category(id_):
    if not require_admin():
        return jsonify({"error": "admin_required"}), 403

    wb = load_wb()
    ws = wb["Categories"]

    for i, row in enumerate(ws.iter_rows(min_row=2), start=2):
        if row[0].value == id_:
            ws.delete_rows(i)
            save_wb(wb)
            make_backup()
            return jsonify({"success": True})

    return jsonify({"error": "not_found"}), 404


# ============================================
# PART 5 — REQUISITIONS MODULE
# ============================================

# ------------------------------
# Currency Conversion
# ------------------------------
def convert_to_usd(amount, currency):
    """
    Convert any currency → USD.
    Uses exchangerate.host API (free, no key needed).
    """
    if currency.upper() == "USD":
        return amount

    try:
        url = f"https://api.exchangerate.host/convert?from={currency}&to=USD&amount={amount}"
        res = requests.get(url).json()
        return float(res.get("result", amount))
    except:
        return amount


# ------------------------------
# LIST ALL REQUISITIONS
# ------------------------------
@app.route("/api/requisitions")
def list_requisitions():
    wb = load_wb()
    ws = wb["Requisitions"]

    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        (
            id_, number, vessel, supplier, date_ord,
            expected, total_usd, paid, category,
            delivered, status, currency, original_amount
        ) = row

        out.append({
            "id": id_,
            "number": number,
            "vessel": vessel,
            "supplier": supplier,
            "date_ordered": date_ord,
            "expected": expected,
            "total_amount": total_usd,
            "paid": paid,
            "category": category,
            "delivered": delivered,
            "status": status,
            "currency": currency,
            "original_amount": original_amount
        })

    return jsonify(out)


# ------------------------------
# LIST DELIVERED REQUISITIONS
# ------------------------------
@app.route("/api/requisitions/delivered")
def list_requisitions_delivered():
    wb = load_wb()
    ws = wb["Requisitions"]

    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[9] == 1:  # delivered column
            id_, number, vessel, supplier, date_ord, exp, tot, paid, cat, deliv, status, cur, orig = row
            out.append({
                "id": id_,
                "number": number,
                "vessel": vessel,
                "supplier": supplier,
                "date_ordered": date_ord,
                "expected": exp,
                "total_amount": tot,
                "paid": paid,
                "category": cat,
                "delivered": deliv,
                "status": status,
                "currency": cur,
                "original_amount": orig
            })

    return jsonify(out)


# ------------------------------
# LIST CANCELLED REQUISITIONS
# ------------------------------
@app.route("/api/requisitions/cancelled")
def list_requisitions_cancelled():
    wb = load_wb()
    ws = wb["Requisitions"]

    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[10] == "cancelled":
            id_, number, vessel, supplier, date_ord, exp, tot, paid, cat, deliv, status, cur, orig = row
            out.append({
                "id": id_,
                "number": number,
                "vessel": vessel,
                "supplier": supplier,
                "date_ordered": date_ord,
                "expected": exp,
                "total_amount": tot,
                "paid": paid,
                "category": cat,
                "delivered": deliv,
                "status": status,
                "currency": cur,
                "original_amount": orig
            })

    return jsonify(out)


# ------------------------------
# ADD REQUISITION
# ------------------------------
@app.route("/api/requisitions", methods=["POST"])
def add_requisition():
    if not require_login():
        return jsonify({"error": "login_required"}), 403

    d = request.json
    number = d.get("number", "").strip()
    vessel = d.get("vessel", "")
    supplier = d.get("supplier", "")
    date_ord = d.get("date_ordered", "")
    expected = d.get("expected", "")
    original_amount = float(d.get("amount", 0))
    currency = d.get("currency", "USD")
    paid = 1 if d.get("paid") else 0
    category = d.get("category", "")
    delivered = 1 if d.get("delivered") else 0

    total_usd = convert_to_usd(original_amount, currency)

    wb = load_wb()
    ws = wb["Requisitions"]

    # Compute next ID
    max_id = 0
    for r in ws.iter_rows(min_row=2, values_only=True):
        if r[0] and r[0] > max_id:
            max_id = r[0]

    ws.append([
        max_id + 1, number, vessel, supplier, date_ord,
        expected, total_usd, paid, category,
        delivered, "open", currency, original_amount
    ])

    save_wb(wb)
    make_backup()

    return jsonify({"success": True})


# ------------------------------
# EDIT REQUISITION
# ------------------------------
@app.route("/api/requisitions/<int:id_>", methods=["PATCH"])
def edit_requisition(id_):
    if not require_login():
        return jsonify({"error": "login_required"}), 403

    d = request.json

    wb = load_wb()
    ws = wb["Requisitions"]

    for row in ws.iter_rows(min_row=2):
        if row[0].value == id_:

            number = d.get("number", row[1].value)
            vessel = d.get("vessel", row[2].value)
            supplier = d.get("supplier", row[3].value)
            date_ord = d.get("date_ordered", row[4].value)
            expected = d.get("expected", row[5].value)

            original_amount = float(d.get("amount", row[12].value))
            currency = d.get("currency", row[11].value)
            paid = 1 if d.get("paid") else 0
            delivered = 1 if d.get("delivered") else 0
            category = d.get("category", row[8].value)
            status = d.get("status", row[10].value)

            total_usd = convert_to_usd(original_amount, currency)

            # Update cells
            row[1].value = number
            row[2].value = vessel
            row[3].value = supplier
            row[4].value = date_ord
            row[5].value = expected
            row[6].value = total_usd
            row[7].value = paid
            row[8].value = category
            row[9].value = delivered
            row[10].value = status
            row[11].value = currency
            row[12].value = original_amount

            save_wb(wb)
            make_backup()
            return jsonify({"success": True})

    return jsonify({"error": "not_found"}), 404


# ------------------------------
# CANCEL / DELETE REQUISITION
# ------------------------------
@app.route("/api/requisitions/<int:id_>", methods=["DELETE"])
def cancel_requisition(id_):
    if not require_login():
        return jsonify({"error": "login_required"}), 403

    wb = load_wb()
    ws = wb["Requisitions"]

    for row in ws.iter_rows(min_row=2):
        if row[0].value == id_:
            row[10].value = "cancelled"
            save_wb(wb)
            make_backup()
            return jsonify({"success": True})

    return jsonify({"error": "not_found"}), 404




# ============================================
# PART 6 — LANDINGS MODULE + BACKUPS API
# ============================================


# -----------------------------------
# LIST ALL LANDINGS
# -----------------------------------
@app.route("/api/landings")
def list_landings():
    wb = load_wb()
    ws = wb["Landings"]

    out = []
    for (
        id_, vessel, item, workshop, amount_usd,
        paid, expected, landed_date, status,
        delivered, currency, original_amount
    ) in ws.iter_rows(min_row=2, values_only=True):

        out.append({
            "id": id_,
            "vessel": vessel,
            "description": item,
            "workshop": workshop,
            "amount": amount_usd,
            "paid": paid,
            "expected": expected,
            "landed_date": landed_date,
            "status": status,
            "delivered": delivered,
            "currency": currency,
            "original_amount": original_amount
        })

    return jsonify(out)


# -----------------------------------
# LIST DELIVERED LANDINGS
# -----------------------------------
@app.route("/api/landings/delivered")
def list_landings_delivered():
    wb = load_wb()
    ws = wb["Landings"]

    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[9] == 1:   # delivered column
            (
                id_, vessel, item, workshop, amount_usd,
                paid, expected, landed_date, status,
                delivered, currency, original_amount
            ) = row

            out.append({
                "id": id_,
                "vessel": vessel,
                "description": item,
                "workshop": workshop,
                "amount": amount_usd,
                "paid": paid,
                "expected": expected,
                "landed_date": landed_date,
                "status": status,
                "delivered": delivered,
                "currency": currency,
                "original_amount": original_amount
            })

    return jsonify(out)


# -----------------------------------
# LIST CANCELLED LANDINGS
# -----------------------------------
@app.route("/api/landings/cancelled")
def list_landings_cancelled():
    wb = load_wb()
    ws = wb["Landings"]

    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[8] == "cancelled":
            (
                id_, vessel, item, workshop, amount_usd,
                paid, expected, landed_date, status,
                delivered, currency, original_amount
            ) = row
            out.append({
                "id": id_,
                "vessel": vessel,
                "description": item,
                "workshop": workshop,
                "amount": amount_usd,
                "paid": paid,
                "expected": expected,
                "landed_date": landed_date,
                "status": status,
                "delivered": delivered,
                "currency": currency,
                "original_amount": original_amount
            })

    return jsonify(out)


# -----------------------------------
# ADD LANDING
# -----------------------------------
@app.route("/api/landings", methods=["POST"])
def add_landing():
    if not require_login():
        return jsonify({"error": "login_required"}), 403

    d = request.json
    vessel = d.get("vessel", "")
    item = d.get("description", "")
    workshop = d.get("workshop", "")
    expected = d.get("expected", "")
    landed_date = d.get("landed_date", "")

    original_amount = float(d.get("amount", 0))
    currency = d.get("currency", "USD")
    paid = 1 if d.get("paid") else 0
    delivered = 1 if d.get("delivered") else 0

    total_usd = convert_to_usd(original_amount, currency)

    wb = load_wb()
    ws = wb["Landings"]

    max_id = 0
    for r in ws.iter_rows(min_row=2, values_only=True):
        if r[0] and r[0] > max_id:
            max_id = r[0]

    ws.append([
        max_id + 1, vessel, item, workshop, total_usd,
        paid, expected, landed_date, "open",
        delivered, currency, original_amount
    ])

    save_wb(wb)
    make_backup()

    return jsonify({"success": True})


# -----------------------------------
# EDIT LANDING
# -----------------------------------
@app.route("/api/landings/<int:id_>", methods=["PATCH"])
def edit_landing(id_):
    if not require_login():
        return jsonify({"error": "login_required"}), 403

    d = request.json
    wb = load_wb()
    ws = wb["Landings"]

    for row in ws.iter_rows(min_row=2):
        if row[0].value == id_:

            vessel = d.get("vessel", row[1].value)
            item = d.get("description", row[2].value)
            workshop = d.get("workshop", row[3].value)

            original_amount = float(d.get("amount", row[11].value))
            currency = d.get("currency", row[10].value)

            expected = d.get("expected", row[6].value)
            landed_date = d.get("landed_date", row[7].value)
            paid = 1 if d.get("paid") else 0
            delivered = 1 if d.get("delivered") else 0
            status = d.get("status", row[8].value)

            usd_amount = convert_to_usd(original_amount, currency)

            row[1].value = vessel
            row[2].value = item
            row[3].value = workshop
            row[4].value = usd_amount
            row[5].value = paid
            row[6].value = expected
            row[7].value = landed_date
            row[8].value = status
            row[9].value = delivered
            row[10].value = currency
            row[11].value = original_amount

            save_wb(wb)
            make_backup()
            return jsonify({"success": True})

    return jsonify({"error": "not_found"}), 404


# -----------------------------------
# CANCEL LANDING
# -----------------------------------
@app.route("/api/landings/<int:id_>", methods=["DELETE"])
def cancel_landing(id_):
    if not require_login():
        return jsonify({"error": "login_required"}), 403

    wb = load_wb()
    ws = wb["Landings"]

    for row in ws.iter_rows(min_row=2):
        if row[0].value == id_:
            row[8].value = "cancelled"
            save_wb(wb)
            make_backup()
            return jsonify({"success": True})

    return jsonify({"error": "not_found"}), 404


# ============================================
# BACKUPS API
# ============================================

@app.route("/api/backups")
def api_backups_list():
    return jsonify(list_backups())


@app.route("/api/backups/download/<filename>")
def api_backups_download(filename):
    path = os.path.join(BACKUP_DIR, filename)
    if not os.path.exists(path):
        return jsonify({"error": "not_found"}), 404
    return send_file(path, as_attachment=True)


@app.route("/api/backups/restore/<filename>", methods=["POST"])
def api_backups_restore(filename):
    ok = restore_backup(filename)
    if not ok:
        return jsonify({"error": "not_found"}), 404
    return jsonify({"success": True})


@app.route("/api/backups/delete/<filename>", methods=["DELETE"])
def api_backups_delete(filename):
    ok = delete_backup(filename)
    if not ok:
        return jsonify({"error": "not_found"}), 404
    return jsonify({"success": True})


# ============================================
# RENDER APP RUNNER
# ============================================
@app.route("/")
def root():
    return jsonify({"message": "AMT Procurement Backend Running"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

