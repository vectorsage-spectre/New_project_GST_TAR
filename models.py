from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash


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


# ---------------- AUDIT TABLE ----------------
from datetime import datetime, timedelta

def ist_now():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


class CaseChange(db.Model):
    __tablename__ = "case_change"

    id = db.Column(db.Integer, primary_key=True)

    case_id = db.Column(db.Integer, nullable=False)
    changed_by = db.Column(db.String(100), nullable=False)

    field_changed = db.Column(db.String(100))
    old_value = db.Column(db.Text)
    new_value = db.Column(db.Text)

    # ✅ IST timestamp
    timestamp = db.Column(db.DateTime, default=ist_now)

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
