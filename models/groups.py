# models/groups.py
from migration_setup import db

class Group(db.Model):
    __tablename__ = 'groups'

    grp_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    grp_name = db.Column(db.String(100), nullable=False)  # Added nullable=False
    grp_status = db.Column(db.Integer, default=1, nullable=False)  # Added nullable=False
    property_ids = db.Column(db.ARRAY(db.Integer), nullable=True)  # Array of integers, nullable
    user_id = db.Column(db.Integer, nullable=False, index=True)  # Added index=True
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp(), nullable=False)
    updated_at = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp(), nullable=False)
    #sample code for implementing indexing
    # Optional: Define a composite index if needed
    __table_args__ = (
        db.Index('idx_user_id_grp_status', 'user_id', 'grp_status'),  # Composite index example
    )