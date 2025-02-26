# models/saved_searches.py
from migration_setup import db
from sqlalchemy.dialects.postgresql import JSONB  # Import JSONB


class BPSavedSearches(db.Model):
    __tablename__ = 'bp_saved_searches'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer)
    search_name = db.Column(db.String(100))
    search_query = db.Column(db.Text)
    search_value = db.Column(JSONB)
    search_response = db.Column(JSONB)
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())
    updated_at = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())
    search_status = db.Column(db.Integer, default=1)