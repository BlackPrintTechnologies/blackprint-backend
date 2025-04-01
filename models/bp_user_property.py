# models/user_property.py
from migration_setup import db
from sqlalchemy.dialects.postgresql import JSONB  # Import JSONB if needed

class BPUserProperty(db.Model):
    __tablename__ = 'bp_user_property'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer, db.ForeignKey('bp_users.bp_user_id'), nullable=False)
    fid = db.Column(db.Integer, nullable=False)
    user_property_status = db.Column(db.String(255))
    status = db.Column(db.Integer, default=1)
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())
    updated_at = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())
    request_status = db.Column(db.Integer, default=0)

    # Define the unique constraint
    __table_args__ = (
        db.UniqueConstraint('user_id', 'fid', 'status', name='unique_user_fid_status'),
    )

    # Define the relationship with the bp_users table
    user = db.relationship('BPUsers', backref='user_properties')

    def __repr__(self):
        return f'<BPUserProperty {self.id}>'