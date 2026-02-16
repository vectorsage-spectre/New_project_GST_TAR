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

CATEGORY_DETAILS = {
    "A1": "Action taken under 79(1)(a)",
    "A2": "Action taken under 79(1)(b)",
    "A3": "Action taken under 79(1)(c)",
    "A4": "Action taken under 79(1)(d)",
    "A5": "Action taken under 79(1)(e)",
    "A6": "Action taken under 79(1)(f)",
    "A7": "Taxpayer/Units not traceable",
    "A8": "Business/Units closed",
    "A9": "Any other arrears recovery",
    "A10": "Action yet to be initiated",
    "SC": "Supreme court pending",
    "HC": "High court pending",
    "GSTAT": "GSTAT pending",
    "Comm_A": "Commissioner Appeals pending",
    "ADC_A": "ADC Appeals pending",
    "WAP": "Within Appeal Period",
    "DIF": "Disposed in Favour of Department",
}

CATEGORY_ORDER = (
    TAR_CATEGORY_MAP["TAR-1"]
    + TAR_CATEGORY_MAP["TAR-2"]
    + TAR_CATEGORY_MAP["TAR-3"]
)

CATEGORY_OPTIONS = [
    {"code": code, "label": f"{code} - {CATEGORY_DETAILS.get(code, '')}".strip(" -")}
    for code in CATEGORY_ORDER
]

CATEGORY_CODES = set(CATEGORY_DETAILS.keys())
DISPOSED_CATEGORY_CODE = "DISP"
DISPOSED_CATEGORY_LABEL = "Arrear Disposed (Archived)"

MOVEMENT_REASONS = {
    "TAR-1": [
        {"code": "T1_DIF", "label": "Decided in favour of Department (Move to TAR-2 DIF)"},
        {"code": "T1_TAXPAYER", "label": "Decided in favour of Taxpayer (Demand Quashed)"},
        {"code": "T1_PART_DEPT", "label": "Decided Partially in favour of Department (Move to TAR-3, update amounts)"},
        {"code": "T1_PART_TAXPAYER", "label": "Decided partly in favour of taxpayer (Quashed Amount)"},
        {"code": "T1_TRANSFER", "label": "Transfer to other jurisdiction / Litigation forum / Remanded Back"},
        {"code": "T1_PAID", "label": "Arrear Realised (Paid in full)"},
        {"code": "T1_REPEAT", "label": "Repeat entry"},
    ],
    "TAR-2": [
        {"code": "T2_APPEAL_FILED", "label": "Appeal filed (Move to TAR-1)"},
        {"code": "T2_NO_APPEAL", "label": "No appeal filed (Move to TAR-3)"},
        {"code": "T2_PAID", "label": "Arrear Realised (Paid in full)"},
        {"code": "T2_TRANSFER", "label": "Transferred to other jurisdiction / other reasons"},
        {"code": "T2_REPEAT", "label": "Repeat entry"},
    ],
    "TAR-3": [
        {"code": "T3_APPEAL_FILED", "label": "Appeal filed (Move to TAR-1)"},
        {"code": "T3_PAID", "label": "Arrears Realised (Paid in full)"},
        {"code": "T3_TRANSFER", "label": "Transferred to other jurisdiction/ Rectification (SPL-05)"},
        {"code": "T3_REPEAT", "label": "Repeat entry"},
    ],
}

NUMERIC_FIELDS = {
    "tax_oio",
    "interest_oio",
    "penalty_oio",
    "total_oio",
    "gst_realised",
    "interest_realised",
    "penalty_realised",
    "total_realised",
    "pending_gst",
    "pending_interest",
    "pending_penalty",
    "pending_total",
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
        dt = pd.to_datetime(value, errors='coerce', dayfirst=True)
        if pd.isna(dt):
            return None
        # Store dates as DD/MM/YYYY text for cross-DB compatibility.
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return None


def seed_master_excel(file_path):
    if not os.path.isabs(file_path):
        file_path = os.path.join(os.path.dirname(__file__), file_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Seed file not found: {file_path}")

    # Reset live case data and related direct mappings before import.
    CaseUserMapping.query.delete()
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

    try:
        for _, row in df.iterrows():
            case = Case()

            for col, (db_field, cleaner) in mapping.items():
                value = cleaner(row.get(col))
                setattr(case, db_field, value)

            db.session.add(case)

        db.session.commit()
        print("Master Excel seeded successfully.")
    except Exception:
        db.session.rollback()
        raise

# PART-2 ---------------------------------------------------------    

# ---------------- IMPORTS ----------------
from flask import Flask, render_template, request, redirect, session, url_for, send_file, jsonify
from datetime import timedelta
from models import (
    db,
    Case,
    CaseChange,
    User,
    MonthlyCategorySnapshot,
    PasswordResetKey,
    UserMappingRule,
    CaseUserMapping,
    UserRecoverySecret,
    SecurityAuditEvent,
    UserNotebook,
    CaseMovementLedger,
    MonthlyReportFinalisation,
    DisposedCase,
)
from flask_migrate import Migrate
from sqlalchemy import func, case as sql_case, text
from io import BytesIO
from fpdf import FPDF
from werkzeug.security import generate_password_hash, check_password_hash
import os
import json
import re
import html
import shutil


# ---------------- APP SETUP ----------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "tar_secret_key")
app.permanent_session_lifetime = timedelta(hours=8)
IST_OFFSET = timedelta(hours=5, minutes=30)


@app.template_filter("ist_datetime")
def ist_datetime(value, fmt="%d-%m-%Y %H:%M"):
    if not value:
        return ""
    return (value + IST_OFFSET).strftime(fmt)


@app.template_filter("num0")
def num0(value):
    # Render numeric-ish values; treat None/blank/invalid as 0.0 for stable UI math.
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s or s.lower() == "none":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0

# ---------------- DATABASE CONFIG ----------------
# Render: set DATABASE_URL (usually Postgres). If absent, fallback to local SQLite in instance/.
db_url = os.environ.get("DATABASE_URL", "").strip()
if db_url.startswith("postgres://"):
    # SQLAlchemy expects postgresql://
    db_url = db_url.replace("postgres://", "postgresql://", 1)
if db_url:
    app.config["SQLALCHEMY_DATABASE_URI"] = db_url
else:
    # Prevent silent data loss on Render restarts when DB URL is missing.
    if os.environ.get("RENDER"):
        raise RuntimeError("DATABASE_URL is required on Render. Attach a Render Postgres database.")
    os.makedirs(app.instance_path, exist_ok=True)
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{os.path.join(app.instance_path, 'gst_tar.db')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
migrate = Migrate(app, db)
with app.app_context():
    db.create_all()

STATE_FILE = os.path.join(os.path.dirname(__file__), "persistent_state.json")
STATE_BOOTSTRAP_DONE = False
BACKUP_ROOT = os.path.join(app.instance_path, "backups")
os.makedirs(BACKUP_ROOT, exist_ok=True)


# ---------------- HOME ROUTE ----------------
@app.route("/")
def home():
    return redirect("/login")



# ---------------- SEED ROUTE ----------------
@app.route("/seed/<key>")
def seed(key):
    if key != "saksham_only_seed":
        return "Not Allowed ❌"
    try:
        seed_master_excel("master_seed.xlsx")
        return "Seeding Done ✅"
    except Exception as e:
        db.session.rollback()
        return f"Seeding failed ❌: {str(e)}", 500


# ----------------- COMMON FILTER FUNCTION -----------------
def mapped_case_query(user_id):
    return (
        Case.query
        .join(CaseUserMapping, CaseUserMapping.case_id == Case.id)
        .filter(CaseUserMapping.user_id == user_id)
    )


def mapped_case_aggregate_query(user_id):
    return (
        db.session.query()
        .select_from(Case)
        .join(CaseUserMapping, CaseUserMapping.case_id == Case.id)
        .filter(CaseUserMapping.user_id == user_id)
    )


def ensure_legacy_assignments_migrated():
    # One-time bootstrap path for non-exclusive mapping table.
    existing_map_count = CaseUserMapping.query.count()
    if existing_map_count > 0:
        return

    # Prefer explicit mapping rules if present.
    rule_count = UserMappingRule.query.count()
    if rule_count > 0:
        apply_saved_mapping_rules()
        return

    # Fallback: seed from legacy Case.assigned_to.
    name_to_user = {u.name: u for u in User.query.all() if u.name}
    legacy_cases = Case.query.filter(Case.assigned_to.isnot(None), Case.assigned_to != "").all()
    for case in legacy_cases:
        target_user = name_to_user.get(case.assigned_to)
        if not target_user:
            continue
        db.session.add(
            CaseUserMapping(
                case_id=case.id,
                user_id=target_user.id,
                mapped_by="legacy-import",
            )
        )
    db.session.commit()


def get_filtered_cases(tar_type, user_id):
    query_text = request.args.get("q", "").strip()
    category = request.args.get("cat", "").strip()
    rng = request.args.get("rng", "").strip()

    q = mapped_case_query(user_id).filter(Case.appeal_status == tar_type)

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
    sort_by = request.args.get("sort", "").strip()
    order = request.args.get("order", "desc")

    # TAR-1 default view: keep recovery categories clubbed in priority order,
    # then sort within each category by pending amount descending.
    if tar_type == "TAR-1" and not sort_by:
        tar1_rank = sql_case(
            (Case.recovery_category == "SC", 1),
            (Case.recovery_category == "HC", 2),
            (Case.recovery_category == "GSTAT", 3),
            (Case.recovery_category == "Comm_A", 4),
            (Case.recovery_category == "ADC_A", 5),
            else_=99,
        )
        q = q.order_by(tar1_rank.asc(), Case.pending_total.desc())
    else:
        effective_sort = sort_by if sort_by else "pending_total"
        column = getattr(Case, effective_sort, Case.id)
        if order == "asc":
            q = q.order_by(column.asc())
        else:
            q = q.order_by(column.desc())

    return q.all()


def clean_form_value(field_name, raw_value):
    if raw_value is None:
        return None

    cleaned = raw_value.strip()
    if not cleaned:
        return None

    if field_name == "recovery_category":
        if cleaned == DISPOSED_CATEGORY_CODE:
            return cleaned
        return cleaned if cleaned in CATEGORY_CODES else None

    if field_name in NUMERIC_FIELDS:
        try:
            return float(cleaned)
        except ValueError:
            return None

    return cleaned


def parse_numeric_optional(raw_value):
    # Returns (value, error). Empty => (None, None). Invalid => (None, error).
    if raw_value is None:
        return None, None
    s = str(raw_value).strip()
    if not s:
        return None, None
    try:
        return float(s), None
    except ValueError:
        return None, "Only numeric values are allowed for amount fields."


def normalize_oio_date(raw_value):
    if raw_value is None:
        return None, None

    value = raw_value.strip()
    if not value:
        return None, None

    # Calendar input can come as YYYY-MM-DD; normalize to DD/MM/YYYY.
    iso_match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", value)
    if iso_match:
        year, month, day = iso_match.groups()
        try:
            datetime(int(year), int(month), int(day))
            return f"{day}/{month}/{year}", None
        except ValueError:
            return None, "Invalid OIO Date. Use DD/MM/YYYY."

    ddmmyyyy_match = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", value)
    if not ddmmyyyy_match:
        return None, "Invalid OIO Date format. Use DD/MM/YYYY."

    day, month, year = ddmmyyyy_match.groups()
    try:
        datetime(int(year), int(month), int(day))
    except ValueError:
        return None, "Invalid OIO Date. Use DD/MM/YYYY."
    return f"{day}/{month}/{year}", None


def parse_oio_date_to_date(value):
    if not value:
        return None

    if hasattr(value, "date") and callable(getattr(value, "date")) and isinstance(value, datetime):
        return value.date()

    # pandas Timestamp / datetime-like
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime().date()
        except Exception:
            pass

    s = str(value).strip()
    if not s:
        return None

    # DD/MM/YYYY
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        dd, mm, yyyy = m.groups()
        try:
            return datetime(int(yyyy), int(mm), int(dd)).date()
        except ValueError:
            return None

    # YYYY-MM-DD
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        yyyy, mm, dd = m.groups()
        try:
            return datetime(int(yyyy), int(mm), int(dd)).date()
        except ValueError:
            return None

    # Common stringified timestamp: YYYY-MM-DD HH:MM:SS
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})\\b", s)
    if m:
        yyyy, mm, dd = m.groups()
        try:
            return datetime(int(yyyy), int(mm), int(dd)).date()
        except ValueError:
            return None

    return None


AUTO_WAP_LAST_RUN_IST_DATE = None


def ist_today_date():
    return (datetime.utcnow() + IST_OFFSET).date()


def allowed_finalisation_window_ist(month_key):
    # Policy: allow finalising a month from 22nd of that month up to 14th of next month (inclusive).
    y, m = month_key.split("-")
    y = int(y)
    m = int(m)
    start = datetime(y, m, 22, 0, 0, 0)  # IST
    if m == 12:
        end = datetime(y + 1, 1, 15, 0, 0, 0)  # exclusive (so up to 14th 23:59)
    else:
        end = datetime(y, m + 1, 15, 0, 0, 0)
    return start, end


def get_month_finalised_end_ist(month_key):
    row = MonthlyReportFinalisation.query.filter_by(snapshot_month=month_key).first()
    if not row:
        return None
    # Stored as UTC datetime. Convert to IST for presentation/windowing.
    return row.finalised_at + IST_OFFSET


def auto_move_wap_to_a10():
    # Rule: If TAR-2 category WAP and OIO Date is older than 90 days, move to TAR-3 A10.
    # Movements are logged in ledger + standard audit trail.
    global AUTO_WAP_LAST_RUN_IST_DATE
    today = ist_today_date()
    if AUTO_WAP_LAST_RUN_IST_DATE == today:
        return
    AUTO_WAP_LAST_RUN_IST_DATE = today

    candidates = Case.query.filter(
        Case.appeal_status == "TAR-2",
        Case.recovery_category == "WAP",
    ).all()

    moved = 0
    for c in candidates:
        od = parse_oio_date_to_date(c.oio_date)
        if not od:
            continue
        if (today - od).days <= 90:
            continue

        from_tar, from_cat = c.appeal_status, c.recovery_category
        c.appeal_status = "TAR-3"
        c.recovery_category = "A10"

        # Keep audit trail
        db.session.add(
            CaseChange(
                case_id=c.id,
                changed_by="SYSTEM",
                field_changed="appeal_status",
                old_value=str(from_tar),
                new_value="TAR-3",
            )
        )
        db.session.add(
            CaseChange(
                case_id=c.id,
                changed_by="SYSTEM",
                field_changed="recovery_category",
                old_value=str(from_cat),
                new_value="A10",
            )
        )

        # Movement ledger
        log_case_movement(
            c,
            moved_by="SYSTEM",
            source="SYSTEM",
            from_tar=from_tar,
            to_tar="TAR-3",
            from_cat=from_cat,
            to_cat="A10",
            note="Auto moved: WAP older than 90 days from OIO Date.",
            reason_code="SYSTEM_AUTO_WAP_90D",
            reason_text="Auto moved: WAP older than 90 days from OIO Date.",
        )
        moved += 1

    if moved:
        db.session.commit()


@app.before_request
def _auto_system_tasks():
    # Lightweight once-per-day task. Keeps WAP cases aging out into A10.
    try:
        auto_move_wap_to_a10()
    except Exception:
        # Avoid breaking request flow if auto task has an issue.
        db.session.rollback()


DERIVED_FINANCIAL_FIELDS = {
    "pending_gst",
    "pending_interest",
    "pending_penalty",
    "pending_total",
    "total_oio",
    "total_realised",
}


def recalc_financials(values):
    # values: dict of field_name -> cleaned value (numeric fields already float)
    tax_oio = float(values.get("tax_oio") or 0.0)
    interest_oio = float(values.get("interest_oio") or 0.0)
    penalty_oio = float(values.get("penalty_oio") or 0.0)

    gst_realised = float(values.get("gst_realised") or 0.0)
    interest_realised = float(values.get("interest_realised") or 0.0)
    penalty_realised = float(values.get("penalty_realised") or 0.0)

    values["pending_gst"] = tax_oio - gst_realised
    values["pending_interest"] = interest_oio - interest_realised
    values["pending_penalty"] = penalty_oio - penalty_realised

    values["total_oio"] = tax_oio + interest_oio + penalty_oio
    values["total_realised"] = gst_realised + interest_realised + penalty_realised
    values["pending_total"] = (
        float(values.get("pending_gst") or 0.0)
        + float(values.get("pending_interest") or 0.0)
        + float(values.get("pending_penalty") or 0.0)
    )

    return values


def append_reason_to_remarks(remarks, reason_text):
    rt = (reason_text or "").strip()
    if not rt:
        return remarks
    base = (remarks or "").strip()
    if not base:
        return rt
    if base.endswith("."):
        return f"{base} {rt}"
    return f"{base}. {rt}"


def archive_and_delete_case(case_obj, disposed_by, reason_code=None, reason_text=None):
    # Copy to disposed archive table, then delete from live tables.
    disposed = DisposedCase(
        original_case_id=case_obj.id,
        disposed_by=str(disposed_by or "SYSTEM"),
        disposed_reason_code=reason_code,
        disposed_reason_text=reason_text,
        original_tar_type=case_obj.appeal_status,
        original_recovery_category=case_obj.recovery_category,
        recovery_category=DISPOSED_CATEGORY_CODE,
        serial_no=case_obj.serial_no,
        party_name=case_obj.party_name,
        gstin_raw=case_obj.gstin_raw,
        oio_display=case_obj.oio_display,
        oio_number=case_obj.oio_number,
        oio_date=case_obj.oio_date,
        issue_brief=case_obj.issue_brief,
        tax_oio=case_obj.tax_oio,
        interest_oio=case_obj.interest_oio,
        penalty_oio=case_obj.penalty_oio,
        total_oio=case_obj.total_oio,
        gst_realised=case_obj.gst_realised,
        interest_realised=case_obj.interest_realised,
        penalty_realised=case_obj.penalty_realised,
        total_realised=case_obj.total_realised,
        predeposit_details=case_obj.predeposit_details,
        pending_gst=case_obj.pending_gst,
        pending_interest=case_obj.pending_interest,
        pending_penalty=case_obj.pending_penalty,
        pending_total=case_obj.pending_total,
        comments=case_obj.comments,
        range_code=case_obj.range_code,
        internal_id=case_obj.internal_id,
        appeal_status=case_obj.appeal_status,
    )
    db.session.add(disposed)

    # Remove mappings
    CaseUserMapping.query.filter_by(case_id=case_obj.id).delete()
    # Delete case itself
    db.session.delete(case_obj)


def is_valid_month_key(value):
    if not value or len(value) != 7 or value[4] != "-":
        return False
    year_part = value[:4]
    month_part = value[5:]
    if not (year_part.isdigit() and month_part.isdigit()):
        return False
    month_number = int(month_part)
    return 1 <= month_number <= 12


def aggregate_category_metrics(user_id, tar_type=None):
    query = (
        db.session.query(
            Case.appeal_status,
            Case.recovery_category,
            func.count(Case.id).label("case_count"),
            func.coalesce(func.sum(Case.total_oio), 0.0).label("total_oio_amount"),
            func.coalesce(func.sum(Case.pending_total), 0.0).label("pending_total_amount"),
        )
        .select_from(Case)
        .join(CaseUserMapping, CaseUserMapping.case_id == Case.id)
        .filter(CaseUserMapping.user_id == user_id)
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


def save_monthly_snapshot(user, snapshot_month):
    metrics = aggregate_category_metrics(user.id)

    MonthlyCategorySnapshot.query.filter_by(
        assigned_to=user.name,
        snapshot_month=snapshot_month
    ).delete()

    for row in metrics:
        db.session.add(
            MonthlyCategorySnapshot(
                snapshot_month=snapshot_month,
                assigned_to=user.name,
                tar_type=row["tar_type"],
                recovery_category=row["recovery_category"],
                case_count=row["case_count"],
                total_oio_amount=row["total_oio_amount"],
                pending_total_amount=row["pending_total_amount"],
            )
        )

    db.session.commit()
    return len(metrics)


def get_session_user():
    return db.session.get(User, session.get("user_id"))


def normalize_username(name):
    return "".join(ch.lower() if ch.isalnum() else "." for ch in name).strip(".")


def log_security_event(event_type, actor_username, target_username, success, details=""):
    db.session.add(
        SecurityAuditEvent(
            event_type=event_type,
            actor_username=actor_username,
            target_username=target_username,
            success=bool(success),
            details=details,
        )
    )


def get_or_create_notebook(user_id):
    nb = UserNotebook.query.filter_by(user_id=user_id).first()
    if nb:
        return nb
    nb = UserNotebook(user_id=user_id, content="")
    db.session.add(nb)
    db.session.commit()
    return nb


def notebook_filename(prefix, username, ext):
    safe_user = "".join(ch for ch in (username or "user") if ch.isalnum() or ch in ("-", "_", "."))
    stamp = datetime.now().strftime("%Y-%m-%d")
    return f"{prefix}_{safe_user}_{stamp}.{ext}"


def sanitize_pdf_text(text):
    # Core fonts support latin-1; replace unsupported chars.
    return (text or "").encode("latin-1", errors="replace").decode("latin-1")

NOTEBOOK_MAX_CHARS = 1000


def notebook_char_count(text):
    return len(text or "")


def notebook_trim_to_max(text, max_chars=NOTEBOOK_MAX_CHARS):
    raw = text or ""
    if len(raw) <= max_chars:
        return raw, False
    return raw[:max_chars], True


def sanitize_backup_name(name):
    raw = (name or "").strip()
    if not raw:
        return None
    # keep it simple: letters/numbers/underscore/dash only
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", raw).strip("_")
    return safe or None


def backup_path(name):
    return os.path.join(BACKUP_ROOT, name)


def create_backup(name, created_by, reason=""):
    if not str(app.config.get("SQLALCHEMY_DATABASE_URI", "")).startswith("sqlite"):
        raise ValueError("Backups are supported only for SQLite deployments.")
    safe = sanitize_backup_name(name)
    if not safe:
        raise ValueError("Invalid backup name.")

    dest = backup_path(safe)
    if os.path.exists(dest):
        # Make unique
        suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        dest = backup_path(f"{safe}_{suffix}")

    os.makedirs(dest, exist_ok=True)
    shutil.copy2(os.path.join(app.instance_path, "gst_tar.db"), os.path.join(dest, "gst_tar.db"))
    if os.path.exists(STATE_FILE):
        shutil.copy2(STATE_FILE, os.path.join(dest, "persistent_state.json"))

    meta = {
        "name": os.path.basename(dest),
        "created_by": created_by,
        "reason": reason or "",
        "created_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
        "created_at_ist": (datetime.utcnow() + IST_OFFSET).isoformat(timespec="seconds"),
    }
    with open(os.path.join(dest, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return meta["name"]


def restore_backup(name):
    if not str(app.config.get("SQLALCHEMY_DATABASE_URI", "")).startswith("sqlite"):
        raise ValueError("Restore is supported only for SQLite deployments.")
    safe = sanitize_backup_name(name)
    if not safe:
        raise ValueError("Invalid backup name.")
    src = backup_path(safe)
    if not os.path.isdir(src):
        raise FileNotFoundError("Backup not found.")

    src_db = os.path.join(src, "gst_tar.db")
    if not os.path.exists(src_db):
        raise FileNotFoundError("Backup DB file not found.")

    # Best-effort: release sqlite handles before overwrite.
    try:
        db.session.remove()
        db.engine.dispose()
    except Exception:
        pass

    shutil.copy2(src_db, os.path.join(app.instance_path, "gst_tar.db"))
    src_state = os.path.join(src, "persistent_state.json")
    if os.path.exists(src_state):
        shutil.copy2(src_state, STATE_FILE)


def clear_trails_day0():
    # Wipe audit/report trails while keeping core seed data/users/mappings.
    CaseChange.query.delete()
    SecurityAuditEvent.query.delete()
    CaseMovementLedger.query.delete()
    MonthlyCategorySnapshot.query.delete()
    MonthlyReportFinalisation.query.delete()
    PasswordResetKey.query.delete()
    UserMappingRule.query.delete()
    db.session.commit()


def ensure_sqlite_columns():
    # Lightweight schema upgrade for SQLite (no migrations required for small additive changes).
    if not str(app.config.get("SQLALCHEMY_DATABASE_URI", "")).startswith("sqlite"):
        return
    try:
        conn = db.engine.raw_connection()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(case_movement_ledger)")
        cols = {row[1] for row in cur.fetchall()}
        if "reason_code" not in cols:
            cur.execute("ALTER TABLE case_movement_ledger ADD COLUMN reason_code TEXT")
        if "reason_text" not in cols:
            cur.execute("ALTER TABLE case_movement_ledger ADD COLUMN reason_text TEXT")
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass


def ensure_postgres_column_sizes():
    # Runtime compatibility fix for already-deployed Postgres schemas.
    uri = str(app.config.get("SQLALCHEMY_DATABASE_URI", "")).lower()
    if not uri.startswith("postgresql"):
        return
    try:
        with db.engine.begin() as conn:
            conn.execute(
                text('ALTER TABLE "case" ALTER COLUMN gstin_verified TYPE VARCHAR(30)')
            )
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass


def log_case_movement(
    case_obj,
    moved_by,
    source,
    from_tar,
    to_tar,
    from_cat,
    to_cat,
    note="",
    reason_code=None,
    reason_text=None,
):
    db.session.add(
        CaseMovementLedger(
            case_id=case_obj.id,
            moved_by=str(moved_by or "SYSTEM"),
            source=str(source or "MANUAL"),
            from_tar_type=from_tar,
            to_tar_type=to_tar,
            from_recovery_category=from_cat,
            to_recovery_category=to_cat,
            reason_code=reason_code,
            reason_text=reason_text,
            pending_total_snapshot=float(case_obj.pending_total or 0.0),
            total_oio_snapshot=float(case_obj.total_oio or 0.0),
            note=note or None,
        )
    )


def export_state_snapshot():
    users = []
    for u in User.query.order_by(User.username.asc()).all():
        users.append({
            "username": u.username,
            "name": u.name,
            "role": u.role,
            "range_code": u.range_code,
            "password_hash": u.password_hash,
        })

    mapping_rules = []
    for r in UserMappingRule.query.order_by(UserMappingRule.created_at.asc()).all():
        target_user = db.session.get(User, r.user_id)
        if not target_user:
            continue
        mapping_rules.append({
            "username": target_user.username,
            "assigned_by": r.assigned_by,
            "tar_type": r.tar_type,
            "range_code": r.range_code,
            "recovery_category": r.recovery_category,
            "matched_case_count": r.matched_case_count,
        })

    case_user_mappings = []
    for m in CaseUserMapping.query.order_by(CaseUserMapping.id.asc()).all():
        target_user = db.session.get(User, m.user_id)
        if not target_user:
            continue
        case_user_mappings.append({
            "case_id": int(m.case_id),
            "username": target_user.username,
            "mapped_by": m.mapped_by,
        })

    return {
        "version": 1,
        "users": users,
        "mapping_rules": mapping_rules,
        "case_user_mappings": case_user_mappings,
    }


def save_state_snapshot():
    data = export_state_snapshot()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def restore_state_snapshot():
    if not os.path.exists(STATE_FILE):
        return

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    users_data = data.get("users", [])
    for row in users_data:
        username = (row.get("username") or "").strip()
        if not username:
            continue
        existing = User.query.filter_by(username=username).first()
        if not existing:
            existing = User(
                username=username,
                name=row.get("name"),
                role=row.get("role") or "RO",
                range_code=row.get("range_code"),
            )
            existing.password_hash = row.get("password_hash") or ""
            db.session.add(existing)
        else:
            if not existing.password_hash and row.get("password_hash"):
                existing.password_hash = row.get("password_hash")
            if not existing.name and row.get("name"):
                existing.name = row.get("name")
            if not existing.role and row.get("role"):
                existing.role = row.get("role")
            if not existing.range_code and row.get("range_code"):
                existing.range_code = row.get("range_code")

    db.session.flush()

    rules_data = data.get("mapping_rules", [])
    existing_signatures = {
        (
            r.user_id,
            r.tar_type or "",
            r.range_code or "",
            r.recovery_category or "",
            r.assigned_by or "",
            int(r.matched_case_count or 0),
        )
        for r in UserMappingRule.query.all()
    }
    for row in rules_data:
        target = User.query.filter_by(username=row.get("username")).first()
        if not target:
            continue
        sig = (
            target.id,
            (row.get("tar_type") or ""),
            (row.get("range_code") or ""),
            (row.get("recovery_category") or ""),
            (row.get("assigned_by") or ""),
            int(row.get("matched_case_count") or 0),
        )
        if sig in existing_signatures:
            continue
        db.session.add(
            UserMappingRule(
                user_id=target.id,
                assigned_by=row.get("assigned_by") or "system",
                tar_type=row.get("tar_type") or None,
                range_code=row.get("range_code") or None,
                recovery_category=row.get("recovery_category") or None,
                matched_case_count=int(row.get("matched_case_count") or 0),
            )
        )

    existing_map = {
        (m.case_id, m.user_id)
        for m in CaseUserMapping.query.all()
    }
    mappings_data = data.get("case_user_mappings", [])
    for row in mappings_data:
        case_id = row.get("case_id")
        username = row.get("username")
        if case_id is None or not username:
            continue
        target_user = User.query.filter_by(username=username).first()
        case = db.session.get(Case, int(case_id))
        if not target_user or not case:
            continue
        sig = (int(case_id), target_user.id)
        if sig in existing_map:
            continue
        db.session.add(
            CaseUserMapping(
                case_id=int(case_id),
                user_id=target_user.id,
                mapped_by=row.get("mapped_by") or "state-restore",
            )
        )

    db.session.commit()


def bootstrap_admin_if_needed():
    # Render deployments typically start with an empty Postgres DB.
    # This ensures first ADMIN exists (env-configurable, with safe defaults).
    global STATE_BOOTSTRAP_DONE
    if getattr(bootstrap_admin_if_needed, "_done", False):
        return

    try:
        user_count = User.query.count()
    except Exception:
        return

    username = (os.environ.get("BOOTSTRAP_ADMIN_USERNAME") or "admin").strip().lower()
    name = (os.environ.get("BOOTSTRAP_ADMIN_NAME") or "Admin").strip()
    # Default password keeps first login possible even if env vars were not configured yet.
    # Change immediately after first login.
    pwd = (os.environ.get("BOOTSTRAP_ADMIN_PASSWORD") or "Admin@12345GST").strip()
    force_reset = (os.environ.get("BOOTSTRAP_ADMIN_FORCE_RESET") or "").strip() in ("1", "true", "TRUE", "yes", "YES")
    if len(pwd) < 6:
        return

    # If DB already has users, bootstrap won't run unless force reset is enabled.
    if user_count > 0 and not force_reset:
        bootstrap_admin_if_needed._done = True
        return

    existing = User.query.filter_by(username=username).first()
    if existing:
        if not force_reset:
            bootstrap_admin_if_needed._done = True
            return
        existing.name = existing.name or name
        existing.role = "ADMIN"
        existing.set_password(pwd)
        u = existing
    else:
        u = User(username=username, name=name, role="ADMIN")
        u.set_password(pwd)
        db.session.add(u)
        db.session.flush()

    pin = (os.environ.get("BOOTSTRAP_ADMIN_RECOVERY_PIN") or "").strip()
    if pin and len(pin) >= 4:
        secret = UserRecoverySecret.query.filter_by(user_id=u.id).first()
        if not secret:
            db.session.add(
                UserRecoverySecret(
                    user_id=u.id,
                    recovery_pin_hash=generate_password_hash(pin),
                )
            )
        else:
            secret.recovery_pin_hash = generate_password_hash(pin)
    db.session.commit()
    print(f"BOOTSTRAP_ADMIN: ensured admin user '{username}' (force_reset={force_reset}, user_count={user_count})")
    bootstrap_admin_if_needed._done = True


@app.before_request
def restore_state_once():
    global STATE_BOOTSTRAP_DONE
    if STATE_BOOTSTRAP_DONE:
        return
    try:
        ensure_sqlite_columns()
        ensure_postgres_column_sizes()
        ensure_legacy_assignments_migrated()
        restore_state_snapshot()
        bootstrap_admin_if_needed()
        STATE_BOOTSTRAP_DONE = True
    except Exception:
        db.session.rollback()


def apply_saved_mapping_rules():
    rules = UserMappingRule.query.order_by(UserMappingRule.created_at.asc()).all()
    if not rules:
        return 0

    total_touched = 0
    for rule in rules:
        target_user = db.session.get(User, rule.user_id)
        if not target_user:
            continue

        query = Case.query
        if rule.tar_type:
            query = query.filter(Case.appeal_status == rule.tar_type)
        if rule.range_code:
            query = query.filter(Case.range_code == rule.range_code)
        if rule.recovery_category:
            query = query.filter(Case.recovery_category == rule.recovery_category)

        matched_cases = query.all()
        for case in matched_cases:
            exists = CaseUserMapping.query.filter_by(case_id=case.id, user_id=target_user.id).first()
            if exists:
                continue
            db.session.add(
                CaseUserMapping(
                    case_id=case.id,
                    user_id=target_user.id,
                    mapped_by="rule-replay",
                )
            )
            total_touched += 1

    db.session.commit()
    return total_touched


def build_archive_month_summary(assigned_to):
    rows = (
        db.session.query(
            MonthlyCategorySnapshot.snapshot_month,
            MonthlyCategorySnapshot.tar_type,
            MonthlyCategorySnapshot.recovery_category,
            func.coalesce(func.sum(MonthlyCategorySnapshot.case_count), 0),
            func.coalesce(func.sum(MonthlyCategorySnapshot.pending_total_amount), 0.0),
        )
        .filter(MonthlyCategorySnapshot.assigned_to == assigned_to)
        .group_by(
            MonthlyCategorySnapshot.snapshot_month,
            MonthlyCategorySnapshot.tar_type,
            MonthlyCategorySnapshot.recovery_category,
        )
        .order_by(
            MonthlyCategorySnapshot.snapshot_month.asc(),
            MonthlyCategorySnapshot.tar_type.asc(),
            MonthlyCategorySnapshot.recovery_category.asc(),
        )
        .all()
    )

    category_rank = {}
    for tar_key, categories in TAR_CATEGORY_MAP.items():
        for index, category in enumerate(categories):
            category_rank[(tar_key, category)] = index

    monthly_map = {}
    for month_key, tar_type, recovery_category, case_count, pending_total_amount in rows:
        if month_key not in monthly_map:
            monthly_map[month_key] = {
                "month": month_key,
                "category_rows": [],
                "final_count": 0,
                "final_pending_lakhs": 0.0,
            }

        pending_lakhs = float((pending_total_amount or 0.0) / 100000.0)
        row_tar = tar_type or "UNSPECIFIED"
        row_category = recovery_category or "UNSPECIFIED"
        monthly_map[month_key]["category_rows"].append({
            "tar_type": row_tar,
            "recovery_category": row_category,
            "count": int(case_count or 0),
            "pending_lakhs": pending_lakhs,
            "_rank": category_rank.get((row_tar, row_category), 999),
        })
        monthly_map[month_key]["final_count"] += int(case_count or 0)
        monthly_map[month_key]["final_pending_lakhs"] += pending_lakhs

    month_list = [monthly_map[key] for key in sorted(monthly_map.keys())]
    for item in month_list:
        item["category_rows"] = sorted(
            item["category_rows"],
            key=lambda r: (r["tar_type"], r["_rank"], r["recovery_category"])
        )

    return month_list



# ----------------- GENERIC LIVE TAR ROUTE -----------------
@app.route("/live/<tar_type>")
def live_tar(tar_type):
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    tar_type = tar_type.upper()
    cases = get_filtered_cases(tar_type, user.id)

    total_count = (
        db.session.query(func.count(Case.id))
        .select_from(Case)
        .join(CaseUserMapping, CaseUserMapping.case_id == Case.id)
        .filter(
            Case.appeal_status == tar_type,
            CaseUserMapping.user_id == user.id
        )
        .scalar()
    )

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
        tar_type=tar_type,
        export_url=url_for("export_live_cases", tar_type=tar_type, **request.args.to_dict()),
        categories=TAR_CATEGORY_MAP.get(tar_type, []),
        total_count=total_count,
        filtered_count=filtered_count,
        has_active_filters=has_active_filters
    )


@app.route("/live/<tar_type>/export")
def export_live_cases(tar_type):
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    tar_type = tar_type.upper()
    cases = get_filtered_cases(tar_type, user.id)

    excluded_export_fields = {"gstin_verified", "oio_number", "oio_date"}
    export_fields = [field for field in FIELD_LABELS.keys() if field not in excluded_export_fields]
    export_labels = [FIELD_LABELS[field] for field in export_fields]

    rows = []
    for case in cases:
        row = {}
        for field, label in zip(export_fields, export_labels):
            value = getattr(case, field, None)
            row[label] = value
        rows.append(row)

    df = pd.DataFrame(rows, columns=export_labels)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=f"{tar_type} Cases")

    output.seek(0)
    filename = f"{tar_type.lower()}_mapped_cases.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
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
            .select_from(Case)
            .join(CaseUserMapping, CaseUserMapping.case_id == Case.id)
            .filter(
                Case.appeal_status == tar_type,
                CaseUserMapping.user_id == user.id
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
        .select_from(Case)
        .join(CaseUserMapping, CaseUserMapping.case_id == Case.id)
        .filter(CaseUserMapping.user_id == user.id)
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

    tar_finals = [
        {
            "tar_type": row["tar_type"],
            "final_count": row["case_count"],
            "final_pending_oio_lakhs": row["pending_total_lakhs"],
        }
        for row in tar_summaries
    ]

    return render_template(
        "tar_report_dashboard.html",
        officer=user.name,
        tar_summaries=tar_summaries,
        category_split=category_split,
        tar_finals=tar_finals,
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

    confirm_text = (request.form.get("confirm_text") or "").strip().upper()
    admin_username = (request.form.get("admin_username") or "").strip().lower()
    admin_password = request.form.get("admin_password") or ""

    if confirm_text != "CONFIRM":
        return redirect("/tar-report-dashboard?msg=confirm_required")

    admin_user = User.query.filter_by(username=admin_username, role="ADMIN").first()
    if not admin_user or not admin_user.check_password(admin_password):
        return redirect("/tar-report-dashboard?msg=invalid_admin_auth")

    saved_rows = save_monthly_snapshot(user, snapshot_month)
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
            row for row in aggregate_category_metrics(user.id, tar_type)
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

    def lakhs(amount):
        return float(amount / 100000.0)

    # Receipts/Disposals window: compare-month start (IST) to today (IST) if compare_month is current month,
    # else to compare-month end (IST).
    def month_start_ist(month_key):
        y, m = month_key.split("-")
        return datetime(int(y), int(m), 1, 0, 0, 0)

    def next_month_start_ist(month_key):
        y, m = month_key.split("-")
        y = int(y)
        m = int(m)
        if m == 12:
            return datetime(y + 1, 1, 1, 0, 0, 0)
        return datetime(y, m + 1, 1, 0, 0, 0)

    now_ist = datetime.utcnow() + IST_OFFSET
    compare_start_ist = month_start_ist(compare_month)
    finalised_end_ist = get_month_finalised_end_ist(compare_month)

    # If admin has finalised the month, use that as the end of the month reporting window.
    # Otherwise: if it's current month, use now; else use end-of-month.
    if finalised_end_ist:
        compare_end_ist = min(now_ist, finalised_end_ist)
    else:
        compare_end_ist = min(now_ist, next_month_start_ist(compare_month))

    window_start_utc = compare_start_ist - IST_OFFSET
    window_end_utc = compare_end_ist - IST_OFFSET
    window_label = f"{compare_month} ({compare_start_ist.strftime('%d-%m-%Y')} to {compare_end_ist.strftime('%d-%m-%Y')})"

    receipts_rows = (
        db.session.query(
            CaseMovementLedger.to_recovery_category,
            func.count(CaseMovementLedger.id),
            func.coalesce(func.sum(CaseMovementLedger.pending_total_snapshot), 0.0),
        )
        .select_from(CaseMovementLedger)
        .join(CaseUserMapping, CaseUserMapping.case_id == CaseMovementLedger.case_id)
        .filter(
            CaseUserMapping.user_id == user.id,
            CaseMovementLedger.to_tar_type == tar_type,
            CaseMovementLedger.moved_at >= window_start_utc,
            CaseMovementLedger.moved_at < window_end_utc,
        )
        .group_by(CaseMovementLedger.to_recovery_category)
        .all()
    )
    disposals_rows = (
        db.session.query(
            CaseMovementLedger.from_recovery_category,
            func.count(CaseMovementLedger.id),
            func.coalesce(func.sum(CaseMovementLedger.pending_total_snapshot), 0.0),
        )
        .select_from(CaseMovementLedger)
        .join(CaseUserMapping, CaseUserMapping.case_id == CaseMovementLedger.case_id)
        .filter(
            CaseUserMapping.user_id == user.id,
            CaseMovementLedger.from_tar_type == tar_type,
            CaseMovementLedger.moved_at >= window_start_utc,
            CaseMovementLedger.moved_at < window_end_utc,
        )
        .group_by(CaseMovementLedger.from_recovery_category)
        .all()
    )

    receipts_map = {
        (cat or "UNSPECIFIED"): {"count": int(cnt or 0), "pending": float(pending or 0.0)}
        for cat, cnt, pending in receipts_rows
    }
    disposals_map = {
        (cat or "UNSPECIFIED"): {"count": int(cnt or 0), "pending": float(pending or 0.0)}
        for cat, cnt, pending in disposals_rows
    }

    def movement_sum(from_tar=None, to_tar=None, reason_codes=None):
        qq = (
            db.session.query(
                func.count(CaseMovementLedger.id),
                func.coalesce(func.sum(CaseMovementLedger.pending_total_snapshot), 0.0),
            )
            .select_from(CaseMovementLedger)
            .join(CaseUserMapping, CaseUserMapping.case_id == CaseMovementLedger.case_id)
            .filter(
                CaseUserMapping.user_id == user.id,
                CaseMovementLedger.moved_at >= window_start_utc,
                CaseMovementLedger.moved_at < window_end_utc,
            )
        )
        if from_tar is not None:
            qq = qq.filter(CaseMovementLedger.from_tar_type == from_tar)
        if to_tar is not None:
            qq = qq.filter(CaseMovementLedger.to_tar_type == to_tar)
        if reason_codes:
            qq = qq.filter(CaseMovementLedger.reason_code.in_(reason_codes))
        cnt, pending = qq.one()
        return int(cnt or 0), float(pending or 0.0)

    opening_count = int(sum(v["case_count"] for v in base_map.values()) if base_map else 0)
    opening_pending_lakhs = lakhs(sum(v["pending_total_amount"] for v in base_map.values()) if base_map else 0.0)

    receipts_count, receipts_pending = movement_sum(to_tar=tar_type)
    receipts_pending_lakhs = lakhs(receipts_pending)

    # Fine report by category (final TAR format).
    def movement_sum_cat(from_tar=None, to_tar=None, reason_codes=None, from_cat=None, to_cat=None):
        qq = (
            db.session.query(
                func.count(CaseMovementLedger.id),
                func.coalesce(func.sum(CaseMovementLedger.pending_total_snapshot), 0.0),
            )
            .select_from(CaseMovementLedger)
            .join(CaseUserMapping, CaseUserMapping.case_id == CaseMovementLedger.case_id)
            .filter(
                CaseUserMapping.user_id == user.id,
                CaseMovementLedger.moved_at >= window_start_utc,
                CaseMovementLedger.moved_at < window_end_utc,
            )
        )
        if from_tar is not None:
            qq = qq.filter(CaseMovementLedger.from_tar_type == from_tar)
        if to_tar is not None:
            qq = qq.filter(CaseMovementLedger.to_tar_type == to_tar)
        if from_cat is not None:
            qq = qq.filter(CaseMovementLedger.from_recovery_category == from_cat)
        if to_cat is not None:
            qq = qq.filter(CaseMovementLedger.to_recovery_category == to_cat)
        if reason_codes:
            qq = qq.filter(CaseMovementLedger.reason_code.in_(reason_codes))
        cnt, pending = qq.one()
        return int(cnt or 0), float(pending or 0.0)

    if tar_type == "TAR-1":
        fine_cols = [
            {"key": "opening", "label": "Opening Balance"},
            {"key": "receipts", "label": "Receipts"},
            {"key": "dif_dept", "label": "Decided in favour of Deptt"},
            {"key": "dif_taxpayer", "label": "Decided in favour of taxpayer"},
            {"key": "part_dept", "label": "Decided partially in favour of Deptt"},
            {"key": "part_taxpayer", "label": "Decided partially in favour of Taxpayer"},
            {"key": "transferred_total", "label": "Transferred Total"},
            {"key": "closing", "label": "Closing Balance"},
        ]
    elif tar_type == "TAR-2":
        fine_cols = [
            {"key": "opening", "label": "Opening Balance"},
            {"key": "receipts", "label": "Receipts"},
            {"key": "appeal_filed", "label": "Appeal filed"},
            {"key": "no_appeal", "label": "No Appeal filed"},
            {"key": "transferred_total", "label": "Transferred Total"},
            {"key": "closing", "label": "Closing Balance"},
        ]
    else:  # TAR-3
        fine_cols = [
            {"key": "opening", "label": "Opening Balance"},
            {"key": "receipts", "label": "Receipts"},
            {"key": "arrears_paid", "label": "Arrears realised (Paid in full)"},
            {"key": "transferred_total", "label": "Transferred Total"},
            {"key": "closing", "label": "Closing Balance"},
        ]

    fine_by_category = []
    fine_totals = {c["key"]: {"count": 0, "pending_lakhs": 0.0} for c in fine_cols}

    for cat in TAR_CATEGORY_MAP.get(tar_type, []):
        opening_c = int(base_map.get(cat, {}).get("case_count", 0))
        opening_p_l = lakhs(float(base_map.get(cat, {}).get("pending_total_amount", 0.0)))

        receipts_c, receipts_p = movement_sum_cat(to_tar=tar_type, to_cat=cat)
        receipts_p_l = lakhs(receipts_p)

        row = {
            "recovery_category": cat,
            "opening": {"count": opening_c, "pending_lakhs": opening_p_l},
            "receipts": {"count": receipts_c, "pending_lakhs": receipts_p_l},
        }

        out_counts = 0
        out_pending = 0.0

        def out_key(key, reason_codes):
            nonlocal out_counts, out_pending
            c, p = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=reason_codes)
            out_counts += c
            out_pending += p
            row[key] = {"count": c, "pending_lakhs": lakhs(p)}

        if tar_type == "TAR-1":
            out_key("dif_dept", ["T1_DIF"])
            out_key("dif_taxpayer", ["T1_TAXPAYER"])
            out_key("part_dept", ["T1_PART_DEPT"])
            out_key("part_taxpayer", ["T1_PART_TAXPAYER"])

            tc, tp = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=["T1_TRANSFER"])
            rc, rp = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=["T1_REPEAT"])
            pc, pp = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=["T1_PAID"])
            transferred_c = tc + rc + pc
            transferred_p = tp + rp + pp
            out_counts += transferred_c
            out_pending += transferred_p
            row["transferred_total"] = {"count": transferred_c, "pending_lakhs": lakhs(transferred_p)}

        elif tar_type == "TAR-2":
            out_key("appeal_filed", ["T2_APPEAL_FILED"])
            out_key("no_appeal", ["T2_NO_APPEAL"])

            tc, tp = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=["T2_TRANSFER"])
            rc, rp = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=["T2_REPEAT"])
            pc, pp = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=["T2_PAID"])
            transferred_c = tc + rc + pc
            transferred_p = tp + rp + pp
            out_counts += transferred_c
            out_pending += transferred_p
            row["transferred_total"] = {"count": transferred_c, "pending_lakhs": lakhs(transferred_p)}

        else:  # TAR-3
            out_key("arrears_paid", ["T3_PAID"])

            ac, ap = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=["T3_APPEAL_FILED"])
            tc, tp = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=["T3_TRANSFER"])
            rc, rp = movement_sum_cat(from_tar=tar_type, from_cat=cat, reason_codes=["T3_REPEAT"])
            transferred_c = ac + tc + rc
            transferred_p = ap + tp + rp
            out_counts += transferred_c
            out_pending += transferred_p
            row["transferred_total"] = {"count": transferred_c, "pending_lakhs": lakhs(transferred_p)}

        closing_c = opening_c + receipts_c - out_counts
        closing_p_l = opening_p_l + receipts_p_l - lakhs(out_pending)
        row["closing"] = {"count": closing_c, "pending_lakhs": closing_p_l}

        # Fill any missing cells with zeros for template simplicity.
        for cdef in fine_cols:
            key = cdef["key"]
            if key not in row:
                row[key] = {"count": 0, "pending_lakhs": 0.0}

        fine_by_category.append(row)

        for cdef in fine_cols:
            key = cdef["key"]
            fine_totals[key]["count"] += int(row[key]["count"] or 0)
            fine_totals[key]["pending_lakhs"] += float(row[key]["pending_lakhs"] or 0.0)

    # Always show all categories for this TAR (even if zeros), and also include any category that appears
    # only via movement events.
    preferred_order = TAR_CATEGORY_MAP.get(tar_type, [])
    category_set = (
        set(preferred_order)
        | set(base_map.keys())
        | set(compare_map.keys())
        | set(receipts_map.keys())
        | set(disposals_map.keys())
    )
    ordered_categories = [c for c in preferred_order if c in category_set]
    ordered_categories += sorted([c for c in category_set if c not in preferred_order])

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

        rec = receipts_map.get(category, {"count": 0, "pending": 0.0})
        disp = disposals_map.get(category, {"count": 0, "pending": 0.0})
        row["receipt_count"] = rec["count"]
        row["receipt_pending_lakhs"] = lakhs(rec["pending"])
        row["disposal_count"] = disp["count"]
        row["disposal_pending_lakhs"] = lakhs(disp["pending"])
        detail_rows.append(row)

        totals["base_count"] += row["base_count"]
        totals["base_total_oio_lakhs"] += row["base_total_oio_lakhs"]
        totals["base_pending_lakhs"] += row["base_pending_lakhs"]
        totals["compare_count"] += row["compare_count"]
        totals["compare_total_oio_lakhs"] += row["compare_total_oio_lakhs"]
        totals["compare_pending_lakhs"] += row["compare_pending_lakhs"]
        totals["receipt_count"] = totals.get("receipt_count", 0) + row["receipt_count"]
        totals["receipt_pending_lakhs"] = totals.get("receipt_pending_lakhs", 0.0) + row["receipt_pending_lakhs"]
        totals["disposal_count"] = totals.get("disposal_count", 0) + row["disposal_count"]
        totals["disposal_pending_lakhs"] = totals.get("disposal_pending_lakhs", 0.0) + row["disposal_pending_lakhs"]

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
        movement_window_label=window_label,
        fine_cols=fine_cols,
        fine_by_category=fine_by_category,
        fine_totals=fine_totals,
        detail_rows=detail_rows,
        totals=totals,
    )


@app.route("/tar-report-archives")
def tar_report_archives():
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    # Keep January as baseline month using current mapped values if missing.
    january_month = "2026-01"
    january_exists = (
        MonthlyCategorySnapshot.query
        .filter_by(assigned_to=user.name, snapshot_month=january_month)
        .first()
    )
    if not january_exists:
        save_monthly_snapshot(user, january_month)

    month_summaries = build_archive_month_summary(user.name)
    return render_template(
        "tar_report_archives.html",
        officer=user.name,
        month_summaries=month_summaries,
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


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        reset_key = (request.form.get("reset_key") or "").strip()
        recovery_pin = (request.form.get("recovery_pin") or "").strip()
        new_password = request.form.get("new_password") or ""
        confirm_password = request.form.get("confirm_password") or ""

        user = User.query.filter_by(username=username).first()
        if not user:
            return render_template("forgot_password.html", error="Invalid username.")

        active_key = (
            PasswordResetKey.query
            .filter_by(user_id=user.id, is_active=True)
            .order_by(PasswordResetKey.created_at.desc())
            .first()
        )
        recovery_secret = UserRecoverySecret.query.filter_by(user_id=user.id).first()

        key_ok = active_key and check_password_hash(active_key.key_hash, reset_key)
        pin_ok = recovery_secret and check_password_hash(recovery_secret.recovery_pin_hash, recovery_pin)
        if not key_ok or not pin_ok:
            log_security_event(
                "FORGOT_PASSWORD_RESET",
                actor_username=username,
                target_username=username,
                success=False,
                details="Failed key/pin validation.",
            )
            db.session.commit()
            return render_template(
                "forgot_password.html",
                error="Invalid forgot password key or recovery PIN."
            )

        if len(new_password) < 6:
            return render_template("forgot_password.html", error="Password must be at least 6 characters.")

        if new_password != confirm_password:
            return render_template("forgot_password.html", error="New password and confirm password do not match.")

        user.set_password(new_password)
        active_key.is_active = False
        log_security_event(
            "FORGOT_PASSWORD_RESET",
            actor_username=username,
            target_username=username,
            success=True,
            details="Password reset using admin key + recovery PIN.",
        )
        db.session.commit()
        return redirect("/login?msg=password_reset")

    return render_template("forgot_password.html")


@app.route("/admin/forgot-key", methods=["GET", "POST"])
def admin_forgot_key():
    user = get_session_user()
    if not user:
        return redirect("/login")
    if user.role != "ADMIN":
        return "Not Allowed ❌"

    users = User.query.order_by(User.username.asc()).all()

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        raw_key = (request.form.get("reset_key") or "").strip()

        target_user = User.query.filter_by(username=username).first()
        if not target_user:
            return render_template("admin_forgot_key.html", users=users, error="User not found.")
        if len(raw_key) < 6:
            return render_template("admin_forgot_key.html", users=users, error="Key must be at least 6 characters.")

        PasswordResetKey.query.filter_by(user_id=target_user.id, is_active=True).update({"is_active": False})
        db.session.add(
            PasswordResetKey(
                user_id=target_user.id,
                key_hash=generate_password_hash(raw_key),
                is_active=True,
            )
        )
        log_security_event(
            "ADMIN_FORGOT_KEY_SET",
            actor_username=user.username,
            target_username=target_user.username,
            success=True,
            details="Admin rotated forgot-password key.",
        )
        db.session.commit()
        return render_template("admin_forgot_key.html", users=users, success=f"Reset key set for {target_user.username}.")

    return render_template("admin_forgot_key.html", users=users)


@app.route("/admin/mapping", methods=["GET", "POST"])
def admin_mapping():
    user = get_session_user()
    if not user:
        return redirect("/login")
    if user.role != "ADMIN":
        return "Not Allowed ❌"

    users = User.query.order_by(User.name.asc()).all()
    mapping_rules = UserMappingRule.query.order_by(UserMappingRule.created_at.desc()).limit(100).all()

    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "create_user":
            name = (request.form.get("name") or "").strip()
            username = (request.form.get("username") or "").strip().lower()
            password = request.form.get("password") or ""
            role = (request.form.get("role") or "RO").strip().upper()

            if not name:
                return render_template(
                    "admin_mapping.html",
                    users=users,
                    mapping_rules=mapping_rules,
                    error="Name is required."
                )

            if not username:
                username = normalize_username(name)

            if not username:
                return render_template(
                    "admin_mapping.html",
                    users=users,
                    mapping_rules=mapping_rules,
                    error="Username is required."
                )

            if User.query.filter_by(username=username).first():
                return render_template(
                    "admin_mapping.html",
                    users=users,
                    mapping_rules=mapping_rules,
                    error=f"Username '{username}' already exists."
                )

            if len(password) < 6:
                return render_template(
                    "admin_mapping.html",
                    users=users,
                    mapping_rules=mapping_rules,
                    error="Password must be at least 6 characters."
                )

            new_user = User(
                name=name,
                username=username,
                role=role if role in ["RO", "ADMIN"] else "RO",
            )
            new_user.set_password(password)
            db.session.add(new_user)
            db.session.commit()
            save_state_snapshot()

            users = User.query.order_by(User.name.asc()).all()
            mapping_rules = UserMappingRule.query.order_by(UserMappingRule.created_at.desc()).limit(100).all()
            return render_template(
                "admin_mapping.html",
                users=users,
                mapping_rules=mapping_rules,
                success=f"User created: {name} ({username})"
            )

        if action == "assign_mapping":
            username = (request.form.get("username") or "").strip()
            tar_type = (request.form.get("tar_type") or "").strip().upper()
            range_code = (request.form.get("range_code") or "").strip()
            recovery_category = (request.form.get("recovery_category") or "").strip()

            target_user = User.query.filter_by(username=username).first()
            if not target_user:
                return render_template(
                    "admin_mapping.html",
                    users=users,
                    mapping_rules=mapping_rules,
                    error="Target user not found."
                )

            query = Case.query
            if tar_type:
                query = query.filter(Case.appeal_status == tar_type)
            if range_code:
                query = query.filter(Case.range_code == range_code)
            if recovery_category:
                query = query.filter(Case.recovery_category == recovery_category)

            matched_cases = query.all()
            total_matched = len(matched_cases)
            newly_mapped = 0
            already_mapped = 0
            for case in matched_cases:
                exists = CaseUserMapping.query.filter_by(case_id=case.id, user_id=target_user.id).first()
                if exists:
                    already_mapped += 1
                    continue
                db.session.add(
                    CaseUserMapping(
                        case_id=case.id,
                        user_id=target_user.id,
                        mapped_by=user.name,
                    )
                )
                newly_mapped += 1

            db.session.add(
                UserMappingRule(
                    user_id=target_user.id,
                    assigned_by=user.name,
                    tar_type=tar_type or None,
                    range_code=range_code or None,
                    recovery_category=recovery_category or None,
                    matched_case_count=total_matched,
                )
            )
            db.session.commit()
            save_state_snapshot()

            users = User.query.order_by(User.name.asc()).all()
            mapping_rules = UserMappingRule.query.order_by(UserMappingRule.created_at.desc()).limit(100).all()
            return render_template(
                "admin_mapping.html",
                users=users,
                mapping_rules=mapping_rules,
                category_options=CATEGORY_OPTIONS,
                success=(
                    f"Mapping result for {target_user.name}: "
                    f"Total matched={total_matched}, Newly mapped={newly_mapped}, Already mapped={already_mapped}."
                )
            )

        if action == "delete_user":
            username = (request.form.get("username") or "").strip()
            target_user = User.query.filter_by(username=username).first()
            if not target_user:
                return render_template(
                    "admin_mapping.html",
                    users=users,
                    mapping_rules=mapping_rules,
                    category_options=CATEGORY_OPTIONS,
                    error="User not found."
                )
            if target_user.id == user.id:
                return render_template(
                    "admin_mapping.html",
                    users=users,
                    mapping_rules=mapping_rules,
                    category_options=CATEGORY_OPTIONS,
                    error="You cannot delete your own logged-in admin account."
                )

            # Keep audit trails in CaseChange table untouched (string-based, no FK).
            PasswordResetKey.query.filter_by(user_id=target_user.id).delete()
            UserRecoverySecret.query.filter_by(user_id=target_user.id).delete()
            UserMappingRule.query.filter_by(user_id=target_user.id).delete()
            CaseUserMapping.query.filter_by(user_id=target_user.id).delete()
            db.session.delete(target_user)

            log_security_event(
                "ADMIN_DELETE_USER",
                actor_username=user.username,
                target_username=username,
                success=True,
                details="User account deleted; audit trail retained.",
            )
            db.session.commit()
            save_state_snapshot()

            users = User.query.order_by(User.name.asc()).all()
            mapping_rules = UserMappingRule.query.order_by(UserMappingRule.created_at.desc()).limit(100).all()
            return render_template(
                "admin_mapping.html",
                users=users,
                mapping_rules=mapping_rules,
                category_options=CATEGORY_OPTIONS,
                success=f"Deleted user account: {username}. Audit trail retained."
            )

        if action == "unmap_filtered":
            username = (request.form.get("username") or "").strip()
            tar_type = (request.form.get("tar_type") or "").strip().upper()
            range_code = (request.form.get("range_code") or "").strip()
            recovery_category = (request.form.get("recovery_category") or "").strip()

            target_user = User.query.filter_by(username=username).first()
            if not target_user:
                return render_template(
                    "admin_mapping.html",
                    users=users,
                    mapping_rules=mapping_rules,
                    category_options=CATEGORY_OPTIONS,
                    error="Target user not found."
                )

            query = (
                db.session.query(CaseUserMapping)
                .join(Case, Case.id == CaseUserMapping.case_id)
                .filter(CaseUserMapping.user_id == target_user.id)
            )
            if tar_type:
                query = query.filter(Case.appeal_status == tar_type)
            if range_code:
                query = query.filter(Case.range_code == range_code)
            if recovery_category:
                query = query.filter(Case.recovery_category == recovery_category)

            rows = query.all()
            deleted_count = 0
            for row in rows:
                db.session.delete(row)
                deleted_count += 1

            log_security_event(
                "ADMIN_UNMAP_FILTERED",
                actor_username=user.username,
                target_username=target_user.username,
                success=True,
                details=f"Removed {deleted_count} mappings (tar={tar_type or 'ALL'}, range={range_code or 'ALL'}, cat={recovery_category or 'ALL'}).",
            )
            db.session.commit()
            save_state_snapshot()

            users = User.query.order_by(User.name.asc()).all()
            mapping_rules = UserMappingRule.query.order_by(UserMappingRule.created_at.desc()).limit(100).all()
            return render_template(
                "admin_mapping.html",
                users=users,
                mapping_rules=mapping_rules,
                category_options=CATEGORY_OPTIONS,
                success=f"Removed {deleted_count} mapping(s) for {target_user.name} with selected filters."
            )

        if action == "unmap_all_user":
            username = (request.form.get("username") or "").strip()
            target_user = User.query.filter_by(username=username).first()
            if not target_user:
                return render_template(
                    "admin_mapping.html",
                    users=users,
                    mapping_rules=mapping_rules,
                    category_options=CATEGORY_OPTIONS,
                    error="Target user not found."
                )

            deleted_count = CaseUserMapping.query.filter_by(user_id=target_user.id).delete()
            log_security_event(
                "ADMIN_UNMAP_ALL_USER",
                actor_username=user.username,
                target_username=target_user.username,
                success=True,
                details=f"Removed all mappings ({deleted_count}).",
            )
            db.session.commit()
            save_state_snapshot()

            users = User.query.order_by(User.name.asc()).all()
            mapping_rules = UserMappingRule.query.order_by(UserMappingRule.created_at.desc()).limit(100).all()
            return render_template(
                "admin_mapping.html",
                users=users,
                mapping_rules=mapping_rules,
                category_options=CATEGORY_OPTIONS,
                success=f"Removed all ({deleted_count}) mappings for {target_user.name}."
            )

    return render_template(
        "admin_mapping.html",
        users=users,
        mapping_rules=mapping_rules,
        category_options=CATEGORY_OPTIONS,
    )


@app.route("/admin/report-finalisation", methods=["GET", "POST"])
def admin_report_finalisation():
    user = get_session_user()
    if not user:
        return redirect("/login")
    if user.role != "ADMIN":
        return "Not Allowed ❌"

    rows = MonthlyReportFinalisation.query.order_by(MonthlyReportFinalisation.snapshot_month.desc()).all()

    if request.method == "POST":
        snapshot_month = (request.form.get("snapshot_month") or "").strip()
        finalised_date = (request.form.get("finalised_date") or "").strip()
        note = (request.form.get("note") or "").strip()

        if not is_valid_month_key(snapshot_month):
            return render_template(
                "admin_report_finalisation.html",
                officer=user.name,
                rows=rows,
                default_month=datetime.now().strftime("%Y-%m"),
                error="Invalid month format. Use YYYY-MM.",
            )

        m = re.fullmatch(r"(\\d{4})-(\\d{2})-(\\d{2})", finalised_date)
        if not m:
            return render_template(
                "admin_report_finalisation.html",
                officer=user.name,
                rows=rows,
                default_month=snapshot_month,
                error="Invalid finalised date format. Use YYYY-MM-DD.",
            )

        y, mo, d = m.groups()
        try:
            finalised_dt_ist = datetime(int(y), int(mo), int(d), 23, 59, 59)
        except ValueError:
            return render_template(
                "admin_report_finalisation.html",
                officer=user.name,
                rows=rows,
                default_month=snapshot_month,
                error="Invalid finalised date.",
            )

        allowed_start, allowed_end_exclusive = allowed_finalisation_window_ist(snapshot_month)
        if not (allowed_start <= finalised_dt_ist < allowed_end_exclusive):
            return render_template(
                "admin_report_finalisation.html",
                officer=user.name,
                rows=rows,
                default_month=snapshot_month,
                error=(
                    f"Finalisation date must be within {allowed_start.strftime('%d-%m-%Y')} "
                    f"to {(allowed_end_exclusive - timedelta(seconds=1)).strftime('%d-%m-%Y')} (IST)."
                ),
            )

        finalised_dt_utc = finalised_dt_ist - IST_OFFSET
        existing = MonthlyReportFinalisation.query.filter_by(snapshot_month=snapshot_month).first()
        if not existing:
            existing = MonthlyReportFinalisation(
                snapshot_month=snapshot_month,
                finalised_at=finalised_dt_utc,
                finalised_by=user.username,
                note=note or None,
            )
            db.session.add(existing)
        else:
            existing.finalised_at = finalised_dt_utc
            existing.finalised_by = user.username
            existing.note = note or None

        db.session.commit()
        # Auto archive a backup for the day/month finalisation was set.
        try:
            create_backup(
                name=f"Finalised_{snapshot_month}",
                created_by=user.username,
                reason="Auto-backup on report month finalisation.",
            )
        except Exception:
            pass
        rows = MonthlyReportFinalisation.query.order_by(MonthlyReportFinalisation.snapshot_month.desc()).all()
        return render_template(
            "admin_report_finalisation.html",
            officer=user.name,
            rows=rows,
            default_month=snapshot_month,
            success=f"Finalisation saved for {snapshot_month}.",
        )

    return render_template(
        "admin_report_finalisation.html",
        officer=user.name,
        rows=rows,
        default_month=datetime.now().strftime("%Y-%m"),
    )


@app.route("/admin/backups", methods=["GET", "POST"])
def admin_backups():
    user = get_session_user()
    if not user:
        return redirect("/login")
    if user.role != "ADMIN":
        return "Not Allowed ❌"

    def list_backups():
        items = []
        if not os.path.isdir(BACKUP_ROOT):
            return items
        for name in sorted(os.listdir(BACKUP_ROOT)):
            path = os.path.join(BACKUP_ROOT, name)
            if not os.path.isdir(path):
                continue
            meta_path = os.path.join(path, "meta.json")
            meta = {"name": name, "created_by": "", "reason": "", "created_at_ist": ""}
            if os.path.exists(meta_path):
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        m = json.load(f)
                        meta["created_by"] = m.get("created_by", "")
                        meta["reason"] = m.get("reason", "")
                        meta["created_at_ist"] = m.get("created_at_ist", "")
                except Exception:
                    pass
            items.append(meta)
        return items

    backups = list_backups()

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        name = (request.form.get("name") or "").strip()
        if action == "create":
            reason = (request.form.get("reason") or "").strip()
            try:
                created_name = create_backup(name=name, created_by=user.username, reason=reason)
            except Exception as e:
                return render_template(
                    "admin_backups.html",
                    officer=user.name,
                    backups=backups,
                    error=str(e),
                )
            backups = list_backups()
            return render_template(
                "admin_backups.html",
                officer=user.name,
                backups=backups,
                success=f"Backup created: {created_name}",
            )

        if action == "restore":
            clear = bool(request.form.get("clear_trails"))
            try:
                restore_backup(name=name)
            except Exception as e:
                return render_template(
                    "admin_backups.html",
                    officer=user.name,
                    backups=backups,
                    error=str(e),
                )

            # After restore, DB content is replaced on disk. Reconnect and optionally clear trails.
            try:
                db.session.remove()
            except Exception:
                pass

            # IMPORTANT: user must restart server for full safety; but we can still clear trails best-effort.
            if clear:
                try:
                    with app.app_context():
                        clear_trails_day0()
                except Exception:
                    pass

            backups = list_backups()
            msg = f"Restored backup: {sanitize_backup_name(name)}. Restart the server now."
            if clear:
                msg += " Trails cleared (Day-0)."
            return render_template(
                "admin_backups.html",
                officer=user.name,
                backups=backups,
                success=msg,
            )

        return render_template(
            "admin_backups.html",
            officer=user.name,
            backups=backups,
            error="Invalid action.",
        )

    return render_template(
        "admin_backups.html",
        officer=user.name,
        backups=backups,
    )


@app.route("/profile")
def profile():
    user = get_session_user()
    if not user:
        return redirect("/login")

    tar_mapping = (
        db.session.query(
            Case.appeal_status,
            func.count(Case.id),
            func.coalesce(func.sum(Case.pending_total), 0.0),
        )
        .select_from(Case)
        .join(CaseUserMapping, CaseUserMapping.case_id == Case.id)
        .filter(CaseUserMapping.user_id == user.id)
        .group_by(Case.appeal_status)
        .order_by(Case.appeal_status.asc())
        .all()
    )

    category_summary = (
        db.session.query(
            Case.recovery_category,
            func.count(Case.id),
            func.coalesce(func.sum(Case.pending_total), 0.0),
        )
        .select_from(Case)
        .join(CaseUserMapping, CaseUserMapping.case_id == Case.id)
        .filter(CaseUserMapping.user_id == user.id)
        .group_by(Case.recovery_category)
        .order_by(Case.recovery_category.asc())
        .all()
    )

    has_recovery_pin = UserRecoverySecret.query.filter_by(user_id=user.id).first() is not None

    # Movement aggregates for this user (manual/create). Stored as moved_by (name/username).
    moved_by_keys = [user.name, user.username]
    movement_agg = (
        db.session.query(
            CaseMovementLedger.from_tar_type,
            CaseMovementLedger.from_recovery_category,
            CaseMovementLedger.to_tar_type,
            CaseMovementLedger.to_recovery_category,
            func.count(CaseMovementLedger.id),
            func.coalesce(func.sum(CaseMovementLedger.pending_total_snapshot), 0.0),
        )
        .select_from(CaseMovementLedger)
        .filter(CaseMovementLedger.moved_by.in_(moved_by_keys))
        .group_by(
            CaseMovementLedger.from_tar_type,
            CaseMovementLedger.from_recovery_category,
            CaseMovementLedger.to_tar_type,
            CaseMovementLedger.to_recovery_category,
        )
        .order_by(func.count(CaseMovementLedger.id).desc())
        .limit(60)
        .all()
    )

    recent_movements = (
        CaseMovementLedger.query
        .filter(CaseMovementLedger.moved_by.in_(moved_by_keys))
        .order_by(CaseMovementLedger.moved_at.desc())
        .limit(60)
        .all()
    )

    return render_template(
        "profile.html",
        officer=user.name,
        username=user.username,
        role=user.role,
        tar_mapping=tar_mapping,
        category_summary=category_summary,
        category_details=CATEGORY_DETAILS,
        has_recovery_pin=has_recovery_pin,
        movement_agg=movement_agg,
        recent_movements=recent_movements,
    )


@app.route("/profile/change-password", methods=["POST"])
def change_password():
    user = get_session_user()
    if not user:
        return redirect("/login")

    current_password = request.form.get("current_password") or ""
    new_password = request.form.get("new_password") or ""
    confirm_password = request.form.get("confirm_password") or ""

    if not user.check_password(current_password):
        log_security_event("CHANGE_PASSWORD", user.username, user.username, False, "Incorrect current password.")
        db.session.commit()
        return redirect("/profile?msg=bad_current")
    if len(new_password) < 6:
        log_security_event("CHANGE_PASSWORD", user.username, user.username, False, "New password too short.")
        db.session.commit()
        return redirect("/profile?msg=short_password")
    if new_password != confirm_password:
        log_security_event("CHANGE_PASSWORD", user.username, user.username, False, "Password confirm mismatch.")
        db.session.commit()
        return redirect("/profile?msg=confirm_mismatch")

    user.set_password(new_password)
    log_security_event("CHANGE_PASSWORD", user.username, user.username, True, "Password changed successfully.")
    db.session.commit()
    return redirect("/profile?msg=password_changed")


@app.route("/profile/change-recovery-pin", methods=["POST"])
def change_recovery_pin():
    user = get_session_user()
    if not user:
        return redirect("/login")

    current_password = request.form.get("current_password_for_pin") or ""
    new_pin = (request.form.get("new_recovery_pin") or "").strip()
    confirm_pin = (request.form.get("confirm_recovery_pin") or "").strip()

    if not user.check_password(current_password):
        log_security_event("CHANGE_RECOVERY_PIN", user.username, user.username, False, "Incorrect current password.")
        db.session.commit()
        return redirect("/profile?msg=pin_bad_current")
    if len(new_pin) < 4:
        log_security_event("CHANGE_RECOVERY_PIN", user.username, user.username, False, "Recovery PIN too short.")
        db.session.commit()
        return redirect("/profile?msg=pin_short")
    if new_pin != confirm_pin:
        log_security_event("CHANGE_RECOVERY_PIN", user.username, user.username, False, "PIN confirm mismatch.")
        db.session.commit()
        return redirect("/profile?msg=pin_mismatch")

    secret = UserRecoverySecret.query.filter_by(user_id=user.id).first()
    if not secret:
        secret = UserRecoverySecret(
            user_id=user.id,
            recovery_pin_hash=generate_password_hash(new_pin),
        )
        db.session.add(secret)
    else:
        secret.recovery_pin_hash = generate_password_hash(new_pin)

    log_security_event("CHANGE_RECOVERY_PIN", user.username, user.username, True, "Recovery PIN updated.")
    db.session.commit()
    return redirect("/profile?msg=pin_changed")


# ------------ CASE UPDATE ROUTE ----------------
@app.route('/case/<int:id>', methods=['GET', 'POST'])
def case_update(id):
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    officer_name = user.name
    case = db.session.get(Case, id)
    if not case:
        return redirect("/tar-report-dashboard")
    has_access = CaseUserMapping.query.filter_by(case_id=case.id, user_id=user.id).first()
    if not has_access:
        return "Not Allowed ❌"
    from_page = request.args.get('from_page', '/live/TAR-3')

    if request.method == 'POST':
        prev_tar = case.appeal_status
        prev_cat = case.recovery_category

        normalized_oio_date, oio_date_error = normalize_oio_date(request.form.get("oio_date"))
        if oio_date_error:
            form_values = {field: request.form.get(field, "") for field in FIELD_LABELS.keys()}
            return render_template(
                'case_view.html',
                case=case,
                officer=user.name,
                labels=FIELD_LABELS,
                category_options=CATEGORY_OPTIONS,
                error=oio_date_error,
                form_values=form_values,
                movement_reasons=MOVEMENT_REASONS.get(case.appeal_status, []),
                current_tar=case.appeal_status,
            )

        # Collect incoming values (typed), validate numerics, then derive pending/totals from OIO and realised.
        new_values = {}
        numeric_error = None
        deliberate_numeric_change = False
        for field in FIELD_LABELS.keys():
            raw_new = request.form.get(field)
            if field in NUMERIC_FIELDS:
                parsed, err = parse_numeric_optional(raw_new)
                if err and not numeric_error:
                    numeric_error = err
                # Deliberate change if user typed something or cleared an existing value.
                if (raw_new or "").strip() != "":
                    deliberate_numeric_change = True
                elif getattr(case, field) is not None:
                    # User cleared the box (blank) while DB had a value.
                    deliberate_numeric_change = True
                new_values[field] = parsed
            else:
                new_values[field] = normalized_oio_date if field == "oio_date" else clean_form_value(field, raw_new)

        if numeric_error:
            form_values = {field: request.form.get(field, "") for field in FIELD_LABELS.keys()}
            return render_template(
                'case_view.html',
                case=case,
                officer=user.name,
                labels=FIELD_LABELS,
                category_options=CATEGORY_OPTIONS,
                error=numeric_error,
                form_values=form_values,
                movement_reasons=MOVEMENT_REASONS.get(case.appeal_status, []),
                current_tar=case.appeal_status,
            )

        # Keep oio_display consistent if number+date exist.
        if new_values.get("oio_number") and new_values.get("oio_date"):
            new_values["oio_display"] = f"{new_values['oio_number']} dated {new_values['oio_date']}"

        recalc_financials(new_values)

        # Detect movement only based on fields actually editable in this form.
        # Currently TAR (appeal_status) isn't editable here, so we only treat recovery_category change as a move.
        intended_new_tar = prev_tar
        intended_new_cat = new_values.get("recovery_category")
        moved = (intended_new_tar != prev_tar) or (intended_new_cat != prev_cat)

        movement_reason_code = (request.form.get("movement_reason") or "").strip()
        allowed_reasons = MOVEMENT_REASONS.get(prev_tar, [])
        reason_map = {r["code"]: r["label"] for r in allowed_reasons}
        if moved:
            if not movement_reason_code or movement_reason_code not in reason_map:
                form_values = {field: request.form.get(field, "") for field in FIELD_LABELS.keys()}
                form_values["movement_reason"] = movement_reason_code
                return render_template(
                    'case_view.html',
                    case=case,
                    officer=user.name,
                    labels=FIELD_LABELS,
                    category_options=CATEGORY_OPTIONS,
                    error=f"Reason for change is mandatory when TAR/Category changes. Select one of the allowed reasons for {prev_tar}.",
                    form_values=form_values,
                    movement_reasons=allowed_reasons,
                    current_tar=prev_tar,
                )

            # Append reason label into remarks/comments for normal category moves.
            # For DISP disposal flow, we append only once inside the disposal handler.
            if intended_new_cat != DISPOSED_CATEGORY_CODE:
                reason_text = reason_map[movement_reason_code]
                new_values["comments"] = append_reason_to_remarks(new_values.get("comments"), reason_text)

        # If user selected "Arrear Disposed", archive and remove from live DB.
        if new_values.get("recovery_category") == DISPOSED_CATEGORY_CODE:
            # Require a movement reason for disposal as well (same rules as category change).
            movement_reason_code = (request.form.get("movement_reason") or "").strip()
            allowed_reasons = MOVEMENT_REASONS.get(prev_tar, [])
            reason_map = {r["code"]: r["label"] for r in allowed_reasons}
            if not movement_reason_code or movement_reason_code not in reason_map:
                form_values = {field: request.form.get(field, "") for field in FIELD_LABELS.keys()}
                form_values["movement_reason"] = movement_reason_code
                return render_template(
                    'case_view.html',
                    case=case,
                    officer=user.name,
                    labels=FIELD_LABELS,
                    category_options=CATEGORY_OPTIONS + [{"code": DISPOSED_CATEGORY_CODE, "label": f"{DISPOSED_CATEGORY_CODE} - {DISPOSED_CATEGORY_LABEL}"}],
                    error=f"Reason for change is mandatory when disposing a case. Select one of the allowed reasons for {prev_tar}.",
                    form_values=form_values,
                    movement_reasons=allowed_reasons,
                    current_tar=prev_tar,
                )

            reason_text = reason_map[movement_reason_code]
            # Append disposal reason to remarks before archiving.
            new_values["comments"] = append_reason_to_remarks(new_values.get("comments"), reason_text)
            case.comments = new_values.get("comments")
            db.session.add(
                CaseChange(
                    case_id=case.id,
                    changed_by=officer_name,
                    field_changed="recovery_category",
                    old_value=str(prev_cat),
                    new_value=DISPOSED_CATEGORY_CODE,
                )
            )
            log_case_movement(
                case,
                moved_by=officer_name,
                source="MANUAL",
                from_tar=prev_tar,
                to_tar="DISPOSED",
                from_cat=prev_cat,
                to_cat=DISPOSED_CATEGORY_CODE,
                note="Case disposed and archived.",
                reason_code=movement_reason_code,
                reason_text=reason_text,
            )
            archive_and_delete_case(
                case,
                disposed_by=officer_name,
                reason_code=movement_reason_code,
                reason_text=reason_text,
            )
            db.session.commit()
            sep = '&' if '?' in from_page else '?'
            return redirect(f"{from_page}{sep}msg=disposed")

        for field in FIELD_LABELS.keys():
            old_value = getattr(case, field)
            new_value = new_values.get(field)

            # Do not silently change DB from None -> 0.0 just because calculations default to 0.0.
            if field in NUMERIC_FIELDS and not deliberate_numeric_change:
                # If the user didn't deliberately edit numeric fields, keep stored values unchanged.
                continue
            if field in DERIVED_FINANCIAL_FIELDS and not deliberate_numeric_change:
                # Derived values should not be stored unless user deliberately edited numeric inputs.
                continue

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

        # If case moved across list (TAR/category), log movement.
        if prev_tar != case.appeal_status or prev_cat != case.recovery_category:
            movement_reason_code = (request.form.get("movement_reason") or "").strip() or None
            reason_text = None
            if movement_reason_code and movement_reason_code in reason_map:
                reason_text = reason_map[movement_reason_code]
            log_case_movement(
                case,
                moved_by=officer_name,
                source="MANUAL",
                from_tar=prev_tar,
                to_tar=case.appeal_status,
                from_cat=prev_cat,
                to_cat=case.recovery_category,
                note="Manual update via Case Update Centre.",
                reason_code=movement_reason_code,
                reason_text=reason_text,
            )

        db.session.commit()

        sep = '&' if '?' in from_page else '?'
        return redirect(f"{from_page}{sep}msg=updated")

    changes = (
        CaseChange.query
        .filter_by(case_id=case.id)
        .order_by(CaseChange.timestamp.desc())
        .limit(200)
        .all()
    )

    return render_template(
        'case_view.html',
        case=case,
        officer=user.name,
        labels=FIELD_LABELS,
        category_options=CATEGORY_OPTIONS + [{"code": DISPOSED_CATEGORY_CODE, "label": f"{DISPOSED_CATEGORY_CODE} - {DISPOSED_CATEGORY_LABEL}"}],
        changes=changes,
        form_values=None,
        movement_reasons=MOVEMENT_REASONS.get(case.appeal_status, []),
        current_tar=case.appeal_status,
    )


@app.route("/audit-trail")
def audit_trail():
    user = get_session_user()
    if not user:
        return redirect("/login")

    q = (request.args.get("q") or "").strip()
    actor = (request.args.get("actor") or "").strip()
    case_id_raw = (request.args.get("case_id") or "").strip()

    query = CaseChange.query
    if user.role != "ADMIN":
        query = query.filter(CaseChange.changed_by == user.name)

    if actor:
        query = query.filter(CaseChange.changed_by.like(f"%{actor}%"))

    if case_id_raw.isdigit():
        query = query.filter(CaseChange.case_id == int(case_id_raw))

    if q:
        like = f"%{q}%"
        query = query.filter(
            (CaseChange.field_changed.like(like)) |
            (CaseChange.old_value.like(like)) |
            (CaseChange.new_value.like(like))
        )

    rows = query.order_by(CaseChange.timestamp.desc()).limit(500).all()
    return render_template(
        "audit_trail.html",
        officer=user.name,
        role=user.role,
        rows=rows,
        labels=FIELD_LABELS,
    )


@app.route("/disposed-cases")
def disposed_cases():
    user = get_session_user()
    if not user:
        return redirect("/login")

    q = DisposedCase.query
    if user.role != "ADMIN":
        q = q.filter(DisposedCase.disposed_by == user.name)

    cases = q.order_by(DisposedCase.disposed_at.desc()).limit(500).all()
    total_count = q.count()
    total_pending = (
        db.session.query(func.coalesce(func.sum(DisposedCase.pending_total), 0.0))
        .select_from(DisposedCase)
        .filter(DisposedCase.id.in_(q.with_entities(DisposedCase.id)))
        .scalar()
    )

    return render_template(
        "disposed_cases.html",
        officer=user.name,
        cases=cases,
        total_count=int(total_count or 0),
        total_pending_lakhs=float((total_pending or 0.0) / 100000.0),
    )


@app.route("/case/new/<tar_type>", methods=["GET", "POST"])
def create_case(tar_type):
    user = db.session.get(User, session.get("user_id"))
    if not user:
        return redirect("/login")

    tar_type = tar_type.upper()
    if tar_type not in TAR_CATEGORY_MAP:
        return redirect("/tar-report-dashboard")

    from_page = request.args.get("from_page", f"/live/{tar_type}")

    if request.method == "POST":
        form_values = {field: request.form.get(field, "") for field in FIELD_LABELS.keys()}
        oio_number = clean_form_value("oio_number", request.form.get("oio_number"))
        normalized_oio_date, oio_date_error = normalize_oio_date(request.form.get("oio_date"))

        if not oio_number:
            return render_template(
                "case_create.html",
                tar_type=tar_type,
                officer=user.name,
                labels=FIELD_LABELS,
                category_options=CATEGORY_OPTIONS,
                from_page=from_page,
                error="OIO No. is mandatory for new case.",
                form_values=form_values,
            )

        if not normalized_oio_date:
            message = oio_date_error or "OIO Date is mandatory for new case in DD/MM/YYYY format."
            return render_template(
                "case_create.html",
                tar_type=tar_type,
                officer=user.name,
                labels=FIELD_LABELS,
                category_options=CATEGORY_OPTIONS,
                from_page=from_page,
                error=message,
                form_values=form_values,
            )

        case = Case()
        case.appeal_status = tar_type

        new_values = {}
        for field in FIELD_LABELS.keys():
            new_values[field] = clean_form_value(field, request.form.get(field))

        new_values["oio_number"] = oio_number
        new_values["oio_date"] = normalized_oio_date
        new_values["oio_display"] = f"{oio_number} dated {normalized_oio_date}"
        recalc_financials(new_values)

        for field in FIELD_LABELS.keys():
            setattr(case, field, new_values.get(field))

        db.session.add(case)
        db.session.flush()

        # Ledger: record addition into this list.
        log_case_movement(
            case,
            moved_by=user.name,
            source="CREATE",
            from_tar=None,
            to_tar=case.appeal_status,
            from_cat=None,
            to_cat=case.recovery_category,
            note="Case created by RO/Admin.",
            reason_code="CREATE",
            reason_text="Case created by RO/Admin.",
        )

        db.session.add(
            CaseUserMapping(
                case_id=case.id,
                user_id=user.id,
                mapped_by=user.name,
            )
        )
        db.session.commit()
        save_state_snapshot()

        sep = '&' if '?' in from_page else '?'
        return redirect(f"{from_page}{sep}msg=created")

    return render_template(
        "case_create.html",
        tar_type=tar_type,
        officer=user.name,
        labels=FIELD_LABELS,
        category_options=CATEGORY_OPTIONS,
        from_page=from_page,
        form_values=None,
    )


# ------------ NOTEBOOK (PERSONAL NOTES) ----------------
@app.route("/notebook")
def notebook():
    user = get_session_user()
    if not user:
        return redirect("/login")

    nb = get_or_create_notebook(user.id)
    trimmed, truncated = notebook_trim_to_max(nb.content or "")
    if truncated and trimmed != (nb.content or ""):
        nb.content = trimmed
        db.session.commit()
    last_saved = nb.updated_at
    return render_template(
        "notebook.html",
        officer=user.name,
        content=nb.content or "",
        last_saved=ist_datetime(last_saved) if last_saved else "",
        char_count=notebook_char_count(nb.content or ""),
        max_chars=NOTEBOOK_MAX_CHARS,
    )


@app.route("/notebook/save", methods=["POST"])
def notebook_save():
    user = get_session_user()
    if not user:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401

    payload = request.get_json(silent=True) or {}
    content = payload.get("content", "")
    if content is None:
        content = ""
    if not isinstance(content, str):
        content = str(content)

    trimmed, truncated = notebook_trim_to_max(content)
    nb = get_or_create_notebook(user.id)
    nb.content = trimmed
    db.session.commit()
    return jsonify({
        "ok": True,
        "saved_at": ist_datetime(nb.updated_at),
        "char_count": notebook_char_count(nb.content or ""),
        "max_chars": NOTEBOOK_MAX_CHARS,
        "truncated": bool(truncated),
    })


@app.route("/notebook/download/<fmt>")
def notebook_download(fmt):
    user = get_session_user()
    if not user:
        return redirect("/login")

    nb = get_or_create_notebook(user.id)
    content = nb.content or ""

    fmt = (fmt or "").lower().strip()
    if fmt == "doc":
        body = html.escape(content).replace("\n", "<br>")
        doc = (
            "<html><head><meta charset='utf-8'></head>"
            "<body>"
            f"<h2>Personal Notebook - {html.escape(user.name or user.username)}</h2>"
            f"<p><i>Exported on {html.escape(datetime.now().strftime('%d-%m-%Y %H:%M'))}</i></p>"
            f"<div style='font-family:Calibri,Arial,sans-serif; font-size:12pt; line-height:1.5;'>{body}</div>"
            "</body></html>"
        )
        filename = notebook_filename("notebook", user.username, "doc")
        return send_file(
            BytesIO(doc.encode("utf-8")),
            as_attachment=True,
            download_name=filename,
            mimetype="application/msword",
        )

    if fmt == "pdf":
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=14)
        pdf.add_page()
        pdf.set_font("Helvetica", size=14)
        title = sanitize_pdf_text(f"Personal Notebook - {user.name or user.username}")
        pdf.multi_cell(0, 8, title)
        pdf.ln(2)
        pdf.set_font("Helvetica", size=10)
        pdf.multi_cell(0, 6, sanitize_pdf_text(f"Exported on {datetime.now().strftime('%d-%m-%Y %H:%M')}"))
        pdf.ln(2)
        pdf.set_font("Helvetica", size=11)

        text = sanitize_pdf_text(content)
        for line in text.splitlines() or [""]:
            pdf.multi_cell(0, 6, line)

        out = BytesIO()
        out.write(pdf.output(dest="S").encode("latin-1"))
        out.seek(0)
        filename = notebook_filename("notebook", user.username, "pdf")
        return send_file(out, as_attachment=True, download_name=filename, mimetype="application/pdf")

    return "Invalid format", 400


# ------------ MOVEMENT LEDGER ----------------
@app.route("/movement-ledger")
def movement_ledger():
    user = get_session_user()
    if not user:
        return redirect("/login")

    # Base query (all movements). Later we can scope to mapped cases if required.
    base_q = CaseMovementLedger.query

    tar_type = (request.args.get("tar_type") or "").strip().upper()
    cat = (request.args.get("cat") or "").strip()
    direction = (request.args.get("direction") or "").strip().lower()

    from_date = (request.args.get("from") or "").strip()
    to_date = (request.args.get("to") or "").strip()

    # Date filters interpret YYYY-MM-DD. We apply to moved_at (UTC) but display IST.
    def parse_ymd(v):
        if not v:
            return None
        m = re.fullmatch(r"(\\d{4})-(\\d{2})-(\\d{2})", v)
        if not m:
            return None
        y, mo, d = m.groups()
        try:
            return datetime(int(y), int(mo), int(d))
        except ValueError:
            return None

    dt_from = parse_ymd(from_date)
    dt_to = parse_ymd(to_date)
    if dt_from:
        base_q = base_q.filter(CaseMovementLedger.moved_at >= dt_from)
    if dt_to:
        base_q = base_q.filter(CaseMovementLedger.moved_at < (dt_to + timedelta(days=1)))

    # "List" filter: tar_type + category. Direction decides whether we match TO (IN) or FROM (OUT).
    q = base_q
    if direction == "in":
        if tar_type:
            q = q.filter(CaseMovementLedger.to_tar_type == tar_type)
        if cat:
            q = q.filter(CaseMovementLedger.to_recovery_category == cat)
    elif direction == "out":
        if tar_type:
            q = q.filter(CaseMovementLedger.from_tar_type == tar_type)
        if cat:
            q = q.filter(CaseMovementLedger.from_recovery_category == cat)
    else:
        if tar_type:
            q = q.filter(
                (CaseMovementLedger.to_tar_type == tar_type) |
                (CaseMovementLedger.from_tar_type == tar_type)
            )
        if cat:
            q = q.filter(
                (CaseMovementLedger.to_recovery_category == cat) |
                (CaseMovementLedger.from_recovery_category == cat)
            )

    movements = q.order_by(CaseMovementLedger.moved_at.desc()).limit(500).all()

    # Summary calculations (in/out) for the selected list filter (tar_type/cat) regardless of chosen direction.
    q_in = base_q
    if tar_type:
        q_in = q_in.filter(CaseMovementLedger.to_tar_type == tar_type)
    if cat:
        q_in = q_in.filter(CaseMovementLedger.to_recovery_category == cat)

    q_out = base_q
    if tar_type:
        q_out = q_out.filter(CaseMovementLedger.from_tar_type == tar_type)
    if cat:
        q_out = q_out.filter(CaseMovementLedger.from_recovery_category == cat)

    in_count = q_in.count()
    out_count = q_out.count()
    in_pending = (
        db.session.query(func.coalesce(func.sum(CaseMovementLedger.pending_total_snapshot), 0.0))
        .select_from(CaseMovementLedger)
        .filter(CaseMovementLedger.id.in_(q_in.with_entities(CaseMovementLedger.id)))
        .scalar()
    )
    out_pending = (
        db.session.query(func.coalesce(func.sum(CaseMovementLedger.pending_total_snapshot), 0.0))
        .select_from(CaseMovementLedger)
        .filter(CaseMovementLedger.id.in_(q_out.with_entities(CaseMovementLedger.id)))
        .scalar()
    )

    summary = {
        "in_count": in_count,
        "out_count": out_count,
        "in_pending_lakhs": float((in_pending or 0.0) / 100000.0),
        "out_pending_lakhs": float((out_pending or 0.0) / 100000.0),
    }

    return render_template(
        "movement_ledger.html",
        officer=user.name,
        movements=movements,
        summary=summary,
        category_options=CATEGORY_OPTIONS,
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
