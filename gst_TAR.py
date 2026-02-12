from flask import Flask
from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from models import db
from models import Case, db
from flask_migrate import Migrate
from flask import Flask, render_template, request, redirect, session



import pandas as pd
from datetime import datetime
from models import User
from models import Case, CaseChange




FIELD_LABELS = {
    "recovery_category": "CATEGORY",
    "serial_no": "Sl.No.",
    "party_name": "Name of the Taxpayer with full address",
    "gstin_raw": "GSTIN No. Whether Active or Not",
    "gstin_verified": "GSTIN_verified",
    "oio_display": "OIO NO. AND DATE",
    "oio_number": "OIO No.",
    "oio_date": "OIO Date",
    "issue_brief": "Issue in brief",
    "tax_oio": "GST Amount OIO",
    "interest_oio": "Interest OIO",
    "penalty_oio": "Penalty OIO",
    "total_oio": "Total OIO",
    "gst_realised": "GST Amount Realised",
    "interest_realised": "Interest Realised",
    "penalty_realised": "Penalty Realised",
    "total_realised": "Total Realised",
    "predeposit_details": "1) Predeposit / 2) Amount other than pre-deposit",
    "pending_gst": "Pending GST",
    "pending_interest": "Pending Interest",
    "pending_penalty": "Pending Penalty",
    "pending_total": "Pending Total",
    "comments": "Remarks / Action taken to realise the arrears and present position and proposed next course of action",
    "range_code": "Range"
}

TAR_CATEGORY_MAP = {
    "TAR-1": ["SC", "HC", "GSTAT", "Comm_A", "ADC_A"],
    "TAR-2": ["WAP", "DIF"],
    "TAR-3": ["A1","A2","A3","A4","A5","A6","A7","A8","A9","A10"]
}



print("GST_TAR FILE LOADED")

def clean_value(value):
    if pd.isna(value):
        return None
    return value


def clean_text(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    if value in ["", "-", "nan"]:
        return None
    return value


def clean_number(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    if value in ["", "-", "nan"]:
        return None
    try:
        return float(value)
    except:
        return None


def clean_date(value):
    if pd.isna(value):
        return None
    try:
        return pd.to_datetime(value, errors='coerce')
    except:
        return None


def seed_master_excel(file_path):
    Case.query.delete()
    db.session.commit()
    df = pd.read_excel(file_path)

    mapping = {
        'CATEGORY': ('recovery_category', clean_text),
        'Sl.No.': ('serial_no', clean_text),
        'Name of the Taxpayer with full address': ('party_name', clean_text),
        'GSTIN No. Whether Active or Not': ('gstin_raw', clean_text),
        'OIO NO. AND DATE': ('oio_display', clean_text),
        'Issue in brief': ('issue_brief', clean_text),
        'GST Amount OIO': ('tax_oio', clean_number),
        'Interest OIO': ('interest_oio', clean_number),
        'Penalty OIO': ('penalty_oio', clean_number),
        'Total OIO': ('total_oio', clean_number),
        'GST Amount Realised': ('gst_realised', clean_number),
        'Interest Realised': ('interest_realised', clean_number),
        'Penalty Realised': ('penalty_realised', clean_number),
        'Total Realised': ('total_realised', clean_number),
        '1) Predeposit / 2) Amount other than pre-deposit': ('predeposit_details', clean_text),
        'Pending GST': ('pending_gst', clean_number),
        'Pending Interest': ('pending_interest', clean_number),
        'Pending Penalty': ('pending_penalty', clean_number),
        'Pending Total': ('pending_total', clean_number),
        'Remarks / Action taken to realise the arrears and present position and proposed next course of action': ('comments', clean_text),
        'Range': ('range_code', clean_text),
        'ID (internal)': ('internal_id', clean_text),
        'OIO No.': ('oio_number', clean_text),
        'OIO Date': ('oio_date', clean_date),
        'GSTIN_verified': ('gstin_verified', clean_text),
        'Report Status': ('appeal_status', clean_text),
        'Date of appeal filing or appeal decision': ('concern_date', clean_date),
    }

    for _, row in df.iterrows():
        case = Case()

        for col, (db_field, cleaner) in mapping.items():
            value = cleaner(row.get(col))
            setattr(case, db_field, value)

        db.session.add(case)

    db.session.commit()
    print("🌱 Master Excel Seeded Successfully!")

# PART-2 ---------------------------------------------------------    

# ---------------- IMPORTS ----------------
from flask import Flask, render_template, request, redirect, session, url_for
from datetime import timedelta
from models import db, Case, CaseChange, User, MonthlyCategorySnapshot
from flask_migrate import Migrate
from sqlalchemy import func


# ---------------- APP SETUP ----------------
app = Flask(__name__)
app.secret_key = "tar_secret_key"
app.permanent_session_lifetime = timedelta(hours=8)

# ---------------- DATABASE CONFIG ----------------
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///gst_tar.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
migrate = Migrate(app, db)
with app.app_context():
    db.create_all()


# ---------------- HOME ROUTE ----------------
@app.route("/")
def home():
    return redirect("/login")



# ---------------- SEED ROUTE ----------------
@app.route("/seed/<key>")
def seed(key):
    if key != "saksham_only_seed":
        return "Not Allowed ❌"

    seed_master_excel("master_seed.xlsx")
    return "Seeding Done ✅"


# ----------------- COMMON FILTER FUNCTION -----------------
def get_filtered_cases(tar_type, officer_name):
    query_text = request.args.get("q", "").strip()
    category = request.args.get("cat", "").strip()
    rng = request.args.get("rng", "").strip()

    q = Case.query.filter(
        Case.appeal_status == tar_type,
        Case.assigned_to == officer_name
    )

    if query_text:
        search = f"%{query_text}%"
        q = q.filter(
            (Case.party_name.like(search)) |
            (Case.gstin_raw.like(search)) |
            (Case.oio_number.like(search))
        )

    if category:
        q = q.filter(Case.recovery_category == category)

    if rng:
        q = q.filter(Case.range_code == rng)

        # -------- Sorting --------
    sort_by = request.args.get("sort", "id")
    order = request.args.get("order", "desc")

    column = getattr(Case, sort_by, Case.id)
    if order == "asc":
        q = q.order_by(column.asc())
    else:
        q = q.order_by(column.desc())

    return q.all()


def is_valid_month_key(value):
    if not value or len(value) != 7 or value[4] != "-":
        return False
    year_part = value[:4]
    month_part = value[5:]
    if not (year_part.isdigit() and month_part.isdigit()):
        return False
    month_number = int(month_part)
    return 1 <= month_number <= 12


def aggregate_category_metrics(assigned_to, tar_type=None):
    query = (
        db.session.query(
            Case.appeal_status,
            Case.recovery_category,
            func.count(Case.id).label("case_count"),
            func.coalesce(func.sum(Case.total_oio), 0.0).label("total_oio_amount"),
            func.coalesce(func.sum(Case.pending_total), 0.0).label("pending_total_amount"),
        )
        .filter(Case.assigned_to == assigned_to)
    )

    if tar_type:
        query = query.filter(Case.appeal_status == tar_type)

    rows = (
        query.group_by(Case.appeal_status, Case.recovery_category)
        .order_by(Case.appeal_status.asc(), Case.recovery_category.asc())
        .all()
    )

    metrics = []
    for row_tar, row_category, row_count, row_total_oio, row_pending in rows:
        metrics.append({
            "tar_type": row_tar or "UNSPECIFIED",
            "recovery_category": row_category or "UNSPECIFIED",
            "case_count": int(row_count or 0),
            "total_oio_amount": float(row_total_oio or 0.0),
            "pending_total_amount": float(row_pending or 0.0),
        })
    return metrics


def save_monthly_snapshot(assigned_to, snapshot_month):
    metrics = aggregate_category_metrics(assigned_to)

    MonthlyCategorySnapshot.query.filter_by(
        assigned_to=assigned_to,
        snapshot_month=snapshot_month
    ).delete()

    for row in metrics:
        db.session.add(
            MonthlyCategorySnapshot(
                snapshot_month=snapshot_month,
                assigned_to=assigned_to,
                tar_type=row["tar_type"],
                recovery_category=row["recovery_category"],
                case_count=row["case_count"],
                total_oio_amount=row["total_oio_amount"],
                pending_total_amount=row["pending_total_amount"],
            )
        )

    db.session.commit()
    return len(metrics)



# ----------------- GENERIC LIVE TAR ROUTE -----------------
@app.route("/live/<tar_type>")
def live_tar(tar_type):
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    tar_type = tar_type.upper()
    cases = get_filtered_cases(tar_type, user.name)

    total_count = Case.query.filter(
        Case.appeal_status == tar_type,
        Case.assigned_to == user.name
    ).count()

    filtered_count = len(cases)
    has_active_filters = any([
        request.args.get("q", "").strip(),
        request.args.get("cat", "").strip(),
        request.args.get("rng", "").strip()
    ])

    return render_template(
        "live_tar.html",
        cases=cases,
        officer=user.name,
        categories=TAR_CATEGORY_MAP.get(tar_type, []),
        total_count=total_count,
        filtered_count=filtered_count,
        has_active_filters=has_active_filters
    )



# ------------ TAR REPORT DASHBOARD ----------------
@app.route("/tar-report-dashboard")
def tar_report_dashboard():
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    def to_lakhs(value):
        return float((value or 0.0) / 100000.0)

    tar_summaries = []
    for tar_type in ["TAR-1", "TAR-2", "TAR-3"]:
        case_count, total_oio_amount, pending_total_amount = (
            db.session.query(
                func.count(Case.id),
                func.coalesce(func.sum(Case.total_oio), 0.0),
                func.coalesce(func.sum(Case.pending_total), 0.0),
            )
            .filter(
                Case.appeal_status == tar_type,
                Case.assigned_to == user.name
            )
            .one()
        )

        tar_summaries.append({
            "tar_type": tar_type,
            "case_count": int(case_count or 0),
            "total_oio_lakhs": to_lakhs(total_oio_amount),
            "pending_total_lakhs": to_lakhs(pending_total_amount),
        })

    category_rows = (
        db.session.query(
            Case.appeal_status,
            Case.recovery_category,
            func.count(Case.id),
            func.coalesce(func.sum(Case.pending_total), 0.0),
        )
        .filter(Case.assigned_to == user.name)
        .group_by(Case.appeal_status, Case.recovery_category)
        .order_by(Case.appeal_status.asc(), Case.recovery_category.asc())
        .all()
    )

    category_split = []
    for tar_type, recovery_category, case_count, pending_total in category_rows:
        category_split.append({
            "tar_type": tar_type or "UNSPECIFIED",
            "recovery_category": recovery_category or "UNSPECIFIED",
            "case_count": int(case_count or 0),
            "pending_total_lakhs": to_lakhs(pending_total),
        })

    return render_template(
        "tar_report_dashboard.html",
        officer=user.name,
        tar_summaries=tar_summaries,
        category_split=category_split,
        default_base_month="2026-01",
        current_month=datetime.now().strftime("%Y-%m"),
    )


@app.route("/tar-report-dashboard/seed-month", methods=["POST"])
def seed_month_snapshot():
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    snapshot_month = request.form.get("snapshot_month", "").strip()
    if not is_valid_month_key(snapshot_month):
        return redirect("/tar-report-dashboard?msg=invalid_month")

    saved_rows = save_monthly_snapshot(user.name, snapshot_month)
    return redirect(
        f"/tar-report-dashboard?msg=seeded&snapshot_month={snapshot_month}&rows={saved_rows}"
    )


@app.route("/tar-report-dashboard/details/<tar_type>")
def tar_report_details(tar_type):
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    tar_type = tar_type.upper()
    if tar_type not in ["TAR-1", "TAR-2", "TAR-3"]:
        return redirect("/tar-report-dashboard")

    base_month = request.args.get("base_month", "2026-01").strip()
    compare_month = request.args.get("compare_month", datetime.now().strftime("%Y-%m")).strip()

    if not is_valid_month_key(base_month):
        base_month = "2026-01"
    if not is_valid_month_key(compare_month):
        compare_month = datetime.now().strftime("%Y-%m")

    base_rows = (
        MonthlyCategorySnapshot.query
        .filter_by(
            assigned_to=user.name,
            tar_type=tar_type,
            snapshot_month=base_month
        )
        .all()
    )
    compare_rows = (
        MonthlyCategorySnapshot.query
        .filter_by(
            assigned_to=user.name,
            tar_type=tar_type,
            snapshot_month=compare_month
        )
        .all()
    )

    base_map = {
        row.recovery_category: {
            "case_count": int(row.case_count or 0),
            "total_oio_amount": float(row.total_oio_amount or 0.0),
            "pending_total_amount": float(row.pending_total_amount or 0.0),
        }
        for row in base_rows
    }

    if compare_rows:
        compare_map = {
            row.recovery_category: {
                "case_count": int(row.case_count or 0),
                "total_oio_amount": float(row.total_oio_amount or 0.0),
                "pending_total_amount": float(row.pending_total_amount or 0.0),
            }
            for row in compare_rows
        }
        compare_source = f"Snapshot {compare_month}"
    else:
        live_rows = [
            row for row in aggregate_category_metrics(user.name, tar_type)
            if row["tar_type"] == tar_type
        ]
        compare_map = {
            row["recovery_category"]: {
                "case_count": row["case_count"],
                "total_oio_amount": row["total_oio_amount"],
                "pending_total_amount": row["pending_total_amount"],
            }
            for row in live_rows
        }
        compare_source = "Live Current Data"

    preferred_order = TAR_CATEGORY_MAP.get(tar_type, [])
    category_set = set(base_map.keys()) | set(compare_map.keys())
    ordered_categories = [c for c in preferred_order if c in category_set]
    ordered_categories += sorted([c for c in category_set if c not in preferred_order])

    def lakhs(amount):
        return float(amount / 100000.0)

    detail_rows = []
    totals = {
        "base_count": 0,
        "base_total_oio_lakhs": 0.0,
        "base_pending_lakhs": 0.0,
        "compare_count": 0,
        "compare_total_oio_lakhs": 0.0,
        "compare_pending_lakhs": 0.0,
    }

    for category in ordered_categories:
        base = base_map.get(category, {"case_count": 0, "total_oio_amount": 0.0, "pending_total_amount": 0.0})
        compare = compare_map.get(category, {"case_count": 0, "total_oio_amount": 0.0, "pending_total_amount": 0.0})

        row = {
            "recovery_category": category,
            "base_count": base["case_count"],
            "base_total_oio_lakhs": lakhs(base["total_oio_amount"]),
            "base_pending_lakhs": lakhs(base["pending_total_amount"]),
            "compare_count": compare["case_count"],
            "compare_total_oio_lakhs": lakhs(compare["total_oio_amount"]),
            "compare_pending_lakhs": lakhs(compare["pending_total_amount"]),
        }
        row["change_count"] = row["compare_count"] - row["base_count"]
        row["change_total_oio_lakhs"] = row["compare_total_oio_lakhs"] - row["base_total_oio_lakhs"]
        row["change_pending_lakhs"] = row["compare_pending_lakhs"] - row["base_pending_lakhs"]
        detail_rows.append(row)

        totals["base_count"] += row["base_count"]
        totals["base_total_oio_lakhs"] += row["base_total_oio_lakhs"]
        totals["base_pending_lakhs"] += row["base_pending_lakhs"]
        totals["compare_count"] += row["compare_count"]
        totals["compare_total_oio_lakhs"] += row["compare_total_oio_lakhs"]
        totals["compare_pending_lakhs"] += row["compare_pending_lakhs"]

    totals["change_count"] = totals["compare_count"] - totals["base_count"]
    totals["change_total_oio_lakhs"] = totals["compare_total_oio_lakhs"] - totals["base_total_oio_lakhs"]
    totals["change_pending_lakhs"] = totals["compare_pending_lakhs"] - totals["base_pending_lakhs"]

    return render_template(
        "tar_report_details.html",
        officer=user.name,
        tar_type=tar_type,
        base_month=base_month,
        compare_month=compare_month,
        compare_source=compare_source,
        detail_rows=detail_rows,
        totals=totals,
    )


# ------------ LOGIN ROUTE ----------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        user = User.query.filter_by(username=username).first()

        if user and user.check_password(password):
            session.clear()
            session.permanent = True
            session["user_id"] = user.id
            return redirect("/tar-report-dashboard")
        else:
            return render_template("login.html", error="Invalid Login. Please try again.")

    return render_template("login.html")


# ------------ CASE UPDATE ROUTE ----------------
@app.route('/case/<int:id>', methods=['GET', 'POST'])
def case_update(id):
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    officer_name = user.name
    case = db.session.get(Case, id)
    from_page = request.args.get('from_page', '/live/TAR-3')

    if request.method == 'POST':
        case.assigned_to = officer_name

        for field in FIELD_LABELS.keys():
            raw_new = request.form.get(field)
            old_value = getattr(case, field)

            if isinstance(old_value, float):
                try:
                    new_value = float(raw_new) if raw_new and raw_new.strip() else None
                except:
                    new_value = None
            else:
                new_value = raw_new.strip() if raw_new and raw_new.strip() else None

            if isinstance(old_value, str):
                old_clean = old_value.strip() if old_value.strip() else None
            else:
                old_clean = old_value

            if old_clean != new_value:
                db.session.add(
                    CaseChange(
                        case_id=case.id,
                        changed_by=officer_name,
                        field_changed=field,
                        old_value=str(old_value),
                        new_value=str(new_value)
                    )
                )
                setattr(case, field, new_value)

        db.session.commit()

        sep = '&' if '?' in from_page else '?'
        return redirect(f"{from_page}{sep}msg=updated")

    changes = (
        CaseChange.query
        .filter_by(case_id=case.id)
        .order_by(CaseChange.timestamp.desc())
        .all()
    )

    return render_template(
        'case_view.html',
        case=case,
        labels=FIELD_LABELS,
        changes=changes
    )


# ----------------- LOGOUT ROUTE --------------------
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


# ---------------- DEBUG ROUTE ----------------
@app.route("/routes")
def show_routes():
    return "<br>".join(str(rule) for rule in app.url_map.iter_rules())


# ---------------- MAIN ----------------
if __name__ == "__main__":
    app.run(debug=True)
