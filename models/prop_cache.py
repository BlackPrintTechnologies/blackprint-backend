# models/prop_cache.py
from migration_setup import db
from sqlalchemy.dialects.postgresql import JSONB  # Import JSONB

class BPPropCache(db.Model):
    __tablename__ = 'bp_prop_cache'

    bpc_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    bpc_fid = db.Column(db.Integer, nullable=False)
    bpc_json = db.Column(JSONB, nullable=False)  # Use JSONB
    bpc_created_at = db.Column(db.DateTime, default=db.func.current_timestamp())
    bpc_status = db.Column(db.Integer, default=1)