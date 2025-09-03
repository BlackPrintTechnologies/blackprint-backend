# models/property_folder.py
from migration_setup import db
from datetime import datetime

class PropertyFolder(db.Model):
    __tablename__ = 'property_folders'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer, db.ForeignKey('bp_users.bp_user_id'), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=True)
    is_default = db.Column(db.Boolean, default=False)  # Default "Liked" folder
    status = db.Column(db.Integer, default=1)  # 1: active, 0: deleted
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = db.relationship('BPUsers', backref='property_folders')
    folder_properties = db.relationship('FolderProperty', backref='folder', cascade='all, delete-orphan')
    
    __table_args__ = (
        db.UniqueConstraint('user_id', 'name', 'status', name='unique_user_folder_name'),
    )
    
    def __repr__(self):
        return f'<PropertyFolder {self.name}>'

class FolderProperty(db.Model):
    __tablename__ = 'folder_properties'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    folder_id = db.Column(db.Integer, db.ForeignKey('property_folders.id'), nullable=False)
    fid = db.Column(db.Integer, nullable=False)  # Property ID
    config_city = db.Column(db.String(50), default='mexico')
    added_at = db.Column(db.DateTime, default=datetime.utcnow)
    notes = db.Column(db.Text, nullable=True)  # User notes about the property
    
    __table_args__ = (
        db.UniqueConstraint('folder_id', 'fid', 'config_city', name='unique_folder_property'),
    )
    
    def __repr__(self):
        return f'<FolderProperty {self.fid} in folder {self.folder_id}>'
