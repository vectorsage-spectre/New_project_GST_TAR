from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import UniqueConstraint


db = SQLAlchemy()


# ---------------- CASE TABLE ----------------
class Case(db.Model):
    __tablename__ = "case"

    id = db.Column(db.Integer, primary_key=True)

    recovery_category = db.Column(db.String(10))
    serial_no = db.Column(db.String(20))
    party_name = db.Column(db.Text)

    gstin_raw = db.Column(db.Text)
    gstin_verified = db.Column(db.String(10))

    oio_display = db.Column(db.Text)
    oio_number = db.Column(db.String(100))
    oio_date = db.Column(db.String)  # will convert to Date later safely

    issue_brief = db.Column(db.Text)

    tax_oio = db.Column(db.Float)
    interest_oio = db.Column(db.Float)
    penalty_oio = db.Column(db.Float)
    total_oio = db.Column(db.Float)

    gst_realised = db.Column(db.Float)
    interest_realised = db.Column(db.Float)
    penalty_realised = db.Column(db.Float)
    total_realised = db.Column(db.Float)

    predeposit_details = db.Column(db.Text)

    pending_gst = db.Column(db.Float)
    pending_interest = db.Column(db.Float)
    pending_penalty = db.Column(db.Float)
    pending_total = db.Column(db.Float)

    comments = db.Column(db.Text)
    range_code = db.Column(db.String(20))
    internal_id = db.Column(db.String(50))

    appeal_status = db.Column(db.String(100))
    concern_date = db.Column(db.Date)

    assigned_to = db.Column(db.String(100))

    def __repr__(self):
        return f"<Case {self.id} - {self.party_name}>"


# ---------------- CASE <-> USER MAPPING (NON-EXCLUSIVE) ----------------
class CaseUserMapping(db.Model):
    __tablename__ = "case_user_mapping"
    __table_args__ = (
        UniqueConstraint("case_id", "user_id", name="uq_case_user_mapping"),
    )

    id = db.Column(db.Integer, primary_key=True)
    case_id = db.Column(db.Integer, nullable=False)
    user_id = db.Column(db.Integer, nullable=False)
    mapped_by = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<CaseUserMapping case_id={self.case_id} user_id={self.user_id}>"


# ---------------- AUDIT TABLE ----------------
class CaseChange(db.Model):
    __tablename__ = "case_change"

    id = db.Column(db.Integer, primary_key=True)

    case_id = db.Column(db.Integer, nullable=False)
    changed_by = db.Column(db.String(100), nullable=False)

    field_changed = db.Column(db.String(100))
    old_value = db.Column(db.Text)
    new_value = db.Column(db.Text)

    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Change Case:{self.case_id} Field:{self.field_changed}>"

# ---------------- USER TABLE (RO LOGIN) ----------------
class User(db.Model):
    __tablename__ = "user"

    id = db.Column(db.Integer, primary_key=True)

    name = db.Column(db.String(100))
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)

    range_code = db.Column(db.String(20))
    role = db.Column(db.String(20), default="RO")  # RO / ADMIN

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f"<User {self.username}>"


# ---------------- MONTHLY CATEGORY SNAPSHOT ----------------
class MonthlyCategorySnapshot(db.Model):
    __tablename__ = "monthly_category_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_month",
            "assigned_to",
            "tar_type",
            "recovery_category",
            name="uq_monthly_category_snapshot",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    snapshot_month = db.Column(db.String(7), nullable=False)  # YYYY-MM
    assigned_to = db.Column(db.String(100), nullable=False)
    tar_type = db.Column(db.String(20), nullable=False)
    recovery_category = db.Column(db.String(50), nullable=False)

    case_count = db.Column(db.Integer, default=0)
    total_oio_amount = db.Column(db.Float, default=0.0)
    pending_total_amount = db.Column(db.Float, default=0.0)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Snapshot {self.snapshot_month} {self.tar_type} {self.recovery_category}>"


# ---------------- FORGOT PASSWORD KEYS ----------------
class PasswordResetKey(db.Model):
    __tablename__ = "password_reset_key"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    key_hash = db.Column(db.String(200), nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<PasswordResetKey user_id={self.user_id} active={self.is_active}>"


# ---------------- USER MAPPING RULES ----------------
class UserMappingRule(db.Model):
    __tablename__ = "user_mapping_rule"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    assigned_by = db.Column(db.String(100), nullable=False)
    tar_type = db.Column(db.String(20))
    range_code = db.Column(db.String(20))
    recovery_category = db.Column(db.String(50))
    matched_case_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<UserMappingRule user_id={self.user_id} cases={self.matched_case_count}>"


# ---------------- USER RECOVERY SECRET ----------------
class UserRecoverySecret(db.Model):
    __tablename__ = "user_recovery_secret"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, unique=True, nullable=False)
    recovery_pin_hash = db.Column(db.String(200), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<UserRecoverySecret user_id={self.user_id}>"


# ---------------- SECURITY AUDIT EVENTS ----------------
class SecurityAuditEvent(db.Model):
    __tablename__ = "security_audit_event"

    id = db.Column(db.Integer, primary_key=True)
    event_type = db.Column(db.String(80), nullable=False)
    actor_username = db.Column(db.String(100))
    target_username = db.Column(db.String(100))
    success = db.Column(db.Boolean, default=False, nullable=False)
    details = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<SecurityAuditEvent {self.event_type} success={self.success}>"


# ---------------- PERSONAL NOTEBOOK (PER USER) ----------------
class UserNotebook(db.Model):
    __tablename__ = "user_notebook"
    __table_args__ = (
        UniqueConstraint("user_id", name="uq_user_notebook_user_id"),
    )

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    content = db.Column(db.Text, default="", nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<UserNotebook user_id={self.user_id}>"


# ---------------- CASE MOVEMENT LEDGER ----------------
class CaseMovementLedger(db.Model):
    __tablename__ = "case_movement_ledger"

    id = db.Column(db.Integer, primary_key=True)
    case_id = db.Column(db.Integer, nullable=False)

    moved_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    moved_by = db.Column(db.String(100), nullable=False)  # username/name or SYSTEM
    source = db.Column(db.String(40), nullable=False)  # MANUAL / SYSTEM / CREATE / IMPORT

    from_tar_type = db.Column(db.String(20))
    to_tar_type = db.Column(db.String(20))
    from_recovery_category = db.Column(db.String(50))
    to_recovery_category = db.Column(db.String(50))

    reason_code = db.Column(db.String(80))
    reason_text = db.Column(db.Text)

    pending_total_snapshot = db.Column(db.Float, default=0.0)
    total_oio_snapshot = db.Column(db.Float, default=0.0)

    note = db.Column(db.Text)

    def __repr__(self):
        return f"<CaseMovementLedger case_id={self.case_id} {self.from_tar_type}/{self.from_recovery_category} -> {self.to_tar_type}/{self.to_recovery_category}>"


# ---------------- DISPOSED CASE ARCHIVE ----------------
class DisposedCase(db.Model):
    __tablename__ = "disposed_case"

    id = db.Column(db.Integer, primary_key=True)
    original_case_id = db.Column(db.Integer, nullable=False)

    disposed_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    disposed_by = db.Column(db.String(100), nullable=False)
    disposed_reason_code = db.Column(db.String(80))
    disposed_reason_text = db.Column(db.Text)

    original_tar_type = db.Column(db.String(100))
    original_recovery_category = db.Column(db.String(50))

    recovery_category = db.Column(db.String(10))
    serial_no = db.Column(db.String(20))
    party_name = db.Column(db.Text)
    gstin_raw = db.Column(db.Text)
    oio_display = db.Column(db.Text)
    oio_number = db.Column(db.String(100))
    oio_date = db.Column(db.String)
    issue_brief = db.Column(db.Text)

    tax_oio = db.Column(db.Float)
    interest_oio = db.Column(db.Float)
    penalty_oio = db.Column(db.Float)
    total_oio = db.Column(db.Float)

    gst_realised = db.Column(db.Float)
    interest_realised = db.Column(db.Float)
    penalty_realised = db.Column(db.Float)
    total_realised = db.Column(db.Float)

    predeposit_details = db.Column(db.Text)

    pending_gst = db.Column(db.Float)
    pending_interest = db.Column(db.Float)
    pending_penalty = db.Column(db.Float)
    pending_total = db.Column(db.Float)

    comments = db.Column(db.Text)
    range_code = db.Column(db.String(20))
    internal_id = db.Column(db.String(50))
    appeal_status = db.Column(db.String(100))

    def __repr__(self):
        return f"<DisposedCase original_case_id={self.original_case_id} disposed_by={self.disposed_by}>"


# ---------------- REPORT FINALISATION (PER MONTH) ----------------
class MonthlyReportFinalisation(db.Model):
    __tablename__ = "monthly_report_finalisation"
    __table_args__ = (
        UniqueConstraint("snapshot_month", name="uq_monthly_report_finalisation_month"),
    )

    id = db.Column(db.Integer, primary_key=True)
    snapshot_month = db.Column(db.String(7), nullable=False)  # YYYY-MM
    finalised_at = db.Column(db.DateTime, nullable=False)  # stored as UTC datetime
    finalised_by = db.Column(db.String(100), nullable=False)
    note = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<MonthlyReportFinalisation {self.snapshot_month} at {self.finalised_at}>"
