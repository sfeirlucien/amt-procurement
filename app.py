from flask import Flask, request, jsonify, session, send_from_directory
import os, time, json, requests
from werkzeug.security import generate_password_hash, check_password_hash
from openpyxl import Workbook, load_workbook

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = os.environ.get("APP_SECRET", "amt-secret")

DATA_FILE = os.environ.get("DATA_FILE", "data.xlsx")

# --------------------------------------------------
# EXCEL SCHEMA (Guaranteed)
# --------------------------------------------------
SHEETS = {
    "Requisitions": [
        "id","number","vessel","category","supplier",
        "date_ordered","expected",
        "original_amount","currency","total_amount",
        "paid","delivered","status"
    ],
    "Landings": [
        "id","vessel","item","workshop",
        "expected","landed_date",
        "amount_original","currency","amount",
        "paid","delivered","status"
    ],
    "Directory": ["id","type","name","email","phone","address"],
    "Categories": ["id","name","abbr"],
    "Users": ["username","password_hash","role"],
    "Vessels": ["id","name"]
}

# --------------------------------------------------
# SAFE EXCEL LOAD + CREATION
# --------------------------------------------------
def open_wb():
    """Load workbook, create if not exists."""
    if not os.path.exists(DATA_FILE):
        wb = Workbook()
        for name, cols in SHEETS.items():
            ws = wb.create_sheet(name)
            ws.append(cols)
        if "Sheet" in wb.sheetnames:
            del wb["Sheet"]
        wb.save(DATA_FILE)

    return load_workbook(DATA_FILE)

def save_wb(wb):
    wb.save(DATA_FILE)

def ensure_sheet_columns(ws, required_cols):
    """Guarantee sheet has exactly required columns."""
    existing = [c.value for c in ws[1]]
    if existing == required_cols:
        return

    existing = [c.value for c in ws[1]]
    col_index = {col: i for i, col in enumerate(existing)} 

    new_rows = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        row_dict = {}
        for col in required_cols:
            idx = col_index.get(col)
            row_dict[col] = r[idx] if idx is not None else None
        new_rows.append(row_dict)

    ws.delete_rows(1, ws.max_row)
    ws.append(required_cols)
    for r in new_rows:
        ws.append([r[c] for c in required_cols])

def load_sheet(name):
    wb = open_wb()
    if name not in wb.sheetnames:
        ws = wb.create_sheet(name)
        ws.append(SHEETS[name])
        save_wb(wb)
    ws = wb[name]
    ensure_sheet_columns(ws, SHEETS[name])

    rows=[]
    for row in ws.iter_rows(min_row=2, values_only=True):
        rows.append(dict(zip(SHEETS[name], row)))
    return rows

def write_sheet(name, rows):
    wb = open_wb()
    if name not in wb.sheetnames:
        ws = wb.create_sheet(name)
        ws.append(SHEETS[name])
    ws = wb[name]

    ws.delete_rows(1, ws.max_row)
    ws.append(SHEETS[name])
    for r in rows:
        ws.append([r.get(c) for c in SHEETS[name]])

    save_wb(wb)

def next_id(rows):
    ids=[int(r["id"]) for r in rows if r.get("id")]
    return max(ids) + 1 if ids else 1

# --------------------------------------------------
# CURRENCY API
# --------------------------------------------------
FX_CACHE = {"ts":0, "rates":{"USD":1.0}}
FX_TTL = 60*60

def get_rates():
    now=time.time()
    if now - FX_CACHE["ts"] < FX_TTL:
        return FX_CACHE["rates"]

    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=8)
        data=r.json()
        rates=data.get("rates")
        if not rates:
            return FX_CACHE["rates"]
        rates["USD"]=1.0
        FX_CACHE["rates"]=rates
        FX_CACHE["ts"]=now
        return rates
    except:
        return FX_CACHE["rates"]

def to_usd(amount, currency):
    if amount is None:
        return 0.0
    try:
        amount=float(amount)
    except:
        return 0.0
    currency=(currency or "USD").upper()
    rate=get_rates().get(currency,1.0)
    return amount / rate

# --------------------------------------------------
# AUTH HELPERS
# --------------------------------------------------
def require_login():
    if not session.get("username"):
        return jsonify({"error":"login_required"}),401
    return None

def require_admin():
    if not session.get("username"):
        return jsonify({"error":"login_required"}),401
    if session.get("role")!="admin":
        return jsonify({"error":"admin_required"}),403
    return None

# --------------------------------------------------
# ROUTES
# --------------------------------------------------

# Static
@app.get("/")
def index():
    return send_from_directory("templates","index.html")

@app.get("/static/<path:p>")
def static_files(p):
    return send_from_directory("static", p)

@app.get("/api/session")
def api_session():
    if session.get("username"):
        return jsonify({
            "username":session["username"],
            "role":session.get("role","user")
        })
    return jsonify({})

@app.post("/api/login")
def api_login():
    data=request.get_json()
    username=data.get("username","").strip()
    password=data.get("password","")

    users=load_sheet("Users")
    u=next((x for x in users if x["username"].lower()==username.lower()),None)
    if not u or not check_password_hash(u["password_hash"],password):
        return jsonify({"error":"invalid_credentials"}),401

    session["username"]=u["username"]
    session["role"]=u["role"]
    return jsonify({"ok":True})

@app.post("/api/logout")
def api_logout():
    session.clear()
    return jsonify({"ok":True})

@app.get("/api/currencies")
def api_currencies():
    return jsonify({"currencies":sorted(list(get_rates().keys()))})

# ----------------- USERS -----------------
@app.get("/api/users")
def users_list():
    err=require_admin()
    if err: return err
    users=load_sheet("Users")
    return jsonify([{"username":u["username"],"role":u["role"]} for u in users])

@app.post("/api/users")
def users_add():
    err=require_admin()
    if err: return err
    data=request.get_json()
    username=data.get("username","").strip()
    password=data.get("password","")
    role=data.get("role","user")

    if not username or not password:
        return jsonify({"error":"bad_request"}),400

    users=load_sheet("Users")
    if any(u["username"].lower()==username.lower() for u in users):
        return jsonify({"error":"duplicate_user"}),400

    users.append({
        "username":username,
        "password_hash":generate_password_hash(password),
        "role":role
    })
    write_sheet("Users",users)
    return jsonify({"ok":True})

@app.delete("/api/users/<username>")
def users_del(username):
    err=require_admin()
    if err: return err

    users=load_sheet("Users")
    users=[u for u in users if u["username"].lower()!=username.lower()]
    write_sheet("Users",users)
    return jsonify({"ok":True})

# ----------------- VESSELS -----------------
@app.get("/api/vessels")
def vessels_list():
    return jsonify(load_sheet("Vessels"))

@app.post("/api/vessels")
def vessels_add():
    err=require_admin()
    if err: return err
    data=request.get_json()
    name=data.get("name","").strip()
    if not name:
        return jsonify({"error":"name_required"}),400

    rows=load_sheet("Vessels")
    if any(r["name"].lower()==name.lower() for r in rows):
        return jsonify({"error":"duplicate_name"}),400

    rows.append({"id":next_id(rows),"name":name})
    write_sheet("Vessels",rows)
    return jsonify({"ok":True})

@app.patch("/api/vessels/<int:vid>")
def vessels_edit(vid):
    err=require_admin()
    if err: return err
    data=request.get_json()
    name=data.get("name","").strip()
    if not name: return jsonify({"error":"name_required"}),400

    rows=load_sheet("Vessels")
    for r in rows:
        if r["id"]==vid:
            r["name"]=name
            break
    write_sheet("Vessels",rows)
    return jsonify({"ok":True})

@app.delete("/api/vessels/<int:vid>")
def vessels_delete(vid):
    err=require_admin()
    if err: return err
    rows=load_sheet("Vessels")
    rows=[r for r in rows if r["id"]!=vid]
    write_sheet("Vessels",rows)
    return jsonify({"ok":True})

# ----------------- CATEGORIES -----------------
@app.get("/api/categories")
def categories():
    return jsonify(load_sheet("Categories"))

@app.post("/api/categories")
def cat_add():
    err=require_admin()
    if err: return err
    data=request.get_json()
    name=data.get("name","").strip()
    abbr=data.get("abbr","").strip()
    if not name or not abbr:
        return jsonify({"error":"bad_request"}),400

    rows=load_sheet("Categories")
    if any(r["abbr"].lower()==abbr.lower() for r in rows):
        return jsonify({"error":"duplicate_abbr"}),400

    rows.append({"id":next_id(rows),"name":name,"abbr":abbr})
    write_sheet("Categories",rows)
    return jsonify({"ok":True})

@app.patch("/api/categories/<int:cid>")
def cat_edit(cid):
    err=require_admin()
    if err: return err
    data=request.get_json()
    name=data.get("name","").strip()
    abbr=data.get("abbr","").strip()
    rows=load_sheet("Categories")
    for r in rows:
        if r["id"]==cid:
            r["name"]=name
            r["abbr"]=abbr
            break
    write_sheet("Categories",rows)
    return jsonify({"ok":True})

@app.delete("/api/categories/<int:cid>")
def cat_delete(cid):
    err=require_admin()
    if err: return err
    rows=load_sheet("Categories")
    rows=[r for r in rows if r["id"]!=cid]
    write_sheet("Categories",rows)
    return jsonify({"ok":True})

# ----------------- DIRECTORY -----------------
@app.get("/api/directory")
def directory():
    t=request.args.get("type")
    rows=load_sheet("Directory")
    if t:
        rows=[r for r in rows if r["type"]==t]
    return jsonify(rows)

@app.post("/api/directory/quick")
def dir_quick():
    err=require_login()
    if err: return err
    data=request.get_json()
    dtype=data.get("type","")
    name=data.get("name","").strip()

    rows=load_sheet("Directory")
    rows.append({
        "id":next_id(rows),
        "type":dtype,
        "name":name,
        "email":data.get("email"),
        "phone":data.get("phone"),
        "address":data.get("address")
    })
    write_sheet("Directory",rows)
    return jsonify({"ok":True})

# ----------------- REQUISITIONS -----------------
@app.get("/api/requisitions")
def req_list():
    return jsonify(load_sheet("Requisitions"))

@app.post("/api/requisitions")
def req_add():
    err=require_login()
    if err: return err
    data=request.get_json()

    rows=load_sheet("Requisitions")
    usd=to_usd(data.get("amount"), data.get("currency"))

    rows.append({
        "id":next_id(rows),
        "number":data.get("number"),
        "vessel":data.get("vessel"),
        "category":data.get("category"),
        "supplier":data.get("supplier"),
        "date_ordered":data.get("date_ordered"),
        "expected":data.get("expected"),
        "original_amount":data.get("amount"),
        "currency":data.get("currency"),
        "total_amount":usd,
        "paid":1 if data.get("paid") else 0,
        "delivered":0,
        "status":"open"
    })
    write_sheet("Requisitions",rows)
    return jsonify({"ok":True})

@app.patch("/api/requisitions/<int:rid>")
def req_edit(rid):
    err=require_login()
    if err: return err
    data=request.get_json()
    rows=load_sheet("Requisitions")

    for r in rows:
        if r["id"]==rid:
            for k in ["number","vessel","category","supplier","date_ordered","expected","status"]:
                if k in data: r[k]=data[k]
            if "paid" in data: r["paid"]=1 if data["paid"] else 0
            if "delivered" in data: r["delivered"]=1 if data["delivered"] else 0

            if "amount" in data or "currency" in data:
                amt=data.get("amount", r["original_amount"])
                cur=data.get("currency", r["currency"])
                r["original_amount"]=amt
                r["currency"]=cur
                r["total_amount"]=to_usd(amt,cur)
            break

    write_sheet("Requisitions",rows)
    return jsonify({"ok":True})

@app.patch("/api/requisitions/<int:rid>/toggle_paid")
def req_toggle_paid(rid):
    err=require_login()
    if err: return err
    rows=load_sheet("Requisitions")
    for r in rows:
        if r["id"]==rid:
            r["paid"]=0 if r["paid"] else 1
            break
    write_sheet("Requisitions",rows)
    return jsonify({"ok":True})

@app.delete("/api/requisitions/<int:rid>")
def req_delete(rid):
    err=require_login()
    if err: return err
    rows=load_sheet("Requisitions")
    rows=[r for r in rows if r["id"]!=rid]
    write_sheet("Requisitions",rows)
    return jsonify({"ok":True})

# ----------------- LANDINGS -----------------
@app.get("/api/landings")
def land_list():
    return jsonify(load_sheet("Landings"))

@app.post("/api/landings")
def land_add():
    err=require_login()
    if err: return err
    data=request.get_json()

    rows=load_sheet("Landings")
    usd=to_usd(data.get("amount"), data.get("currency"))

    rows.append({
        "id":next_id(rows),
        "vessel":data.get("vessel"),
        "item":data.get("item"),
        "workshop":data.get("workshop"),
        "expected":data.get("expected"),
        "landed_date":data.get("landed_date"),
        "amount_original":data.get("amount"),
        "currency":data.get("currency"),
        "amount":usd,
        "paid":1 if data.get("paid") else 0,
        "delivered":0,
        "status":"open"
    })
    write_sheet("Landings",rows)
    return jsonify({"ok":True})

@app.patch("/api/landings/<int:lid>")
def land_edit(lid):
    err=require_login()
    if err: return err
    data=request.get_json()
    rows=load_sheet("Landings")

    for r in rows:
        if r["id"]==lid:
            for k in ["vessel","item","workshop","expected","landed_date","status"]:
                if k in data: r[k]=data[k]
            if "paid" in data: r["paid"]=1 if data["paid"] else 0
            if "delivered" in data: r["delivered"]=1 if data["delivered"] else 0

            if "amount" in data or "currency" in data:
                amt=data.get("amount", r["amount_original"])
                cur=data.get("currency", r["currency"])
                r["amount_original"]=amt
                r["currency"]=cur
                r["amount"]=to_usd(amt,cur)
            break

    write_sheet("Landings",rows)
    return jsonify({"ok":True})

@app.patch("/api/landings/<int:lid>/toggle_paid")
def land_toggle_paid(lid):
    err=require_login()
    if err: return err
    rows=load_sheet("Landings")
    for r in rows:
        if r["id"]==lid:
            r["paid"]=0 if r["paid"] else 1
            break
    write_sheet("Landings",rows)
    return jsonify({"ok":True})

@app.delete("/api/landings/<int:lid>")
def land_delete(lid):
    err=require_login()
    if err: return err
    rows=load_sheet("Landings")
    rows=[r for r in rows if r["id"]!=lid]
    write_sheet("Landings",rows)
    return jsonify({"ok":True})


# --------------------------------------------------
# RUN
# --------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)
