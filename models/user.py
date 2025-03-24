# models/user.py
from migration_setup import db  # Import db from migration_setup.py

class BPUser(db.Model):
    __tablename__ = 'bp_users'

    bp_user_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    bp_name = db.Column(db.String(255), nullable=False)
    bp_company = db.Column(db.String(255))
    bp_industry = db.Column(db.String(255))
    bp_email = db.Column(db.String(255), nullable=False)
    bp_password = db.Column(db.String(255), nullable=False)
    bp_created_on = db.Column(db.DateTime, default=db.func.current_timestamp())
    bp_status = db.Column(db.Integer, default=1)
    bp_is_onboarded = db.Column(db.Integer, default=0)
    bp_user_verified = db.Column(db.Integer, default=0)