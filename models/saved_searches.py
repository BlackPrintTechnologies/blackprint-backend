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
    
    # New fields for location-based search
    city = db.Column(db.String(100))
    municipality = db.Column(db.String(100))
    colonia = db.Column(db.String(100))
    zip_code = db.Column(db.String(20))
    
    # New fields for property filters
    property_types = db.Column(JSONB)  # Array of selected property types
    availability = db.Column(db.String(50))  # "Show on-market", "Show off-market", etc.
    plot_dimensions_min = db.Column(db.Numeric(15, 2))  # Minimum plot area in m²
    plot_dimensions_max = db.Column(db.Numeric(15, 2))  # Maximum plot area in m²
    construction_dimensions_min = db.Column(db.Numeric(15, 2))  # Minimum construction area in m²
    construction_dimensions_max = db.Column(db.Numeric(15, 2))  # Maximum construction area in m²
    
    # Price and transaction fields
    price_min = db.Column(db.Numeric(15, 2))  # Minimum price in MXN
    price_max = db.Column(db.Numeric(15, 2))  # Maximum price in MXN
    transaction_type = db.Column(db.String(20))  # "Buy" or "Rent"
    
    # Search metadata
    search_type = db.Column(db.String(50))  # "Direct Search", "Filter Search", "Layer Search"
    search_status = db.Column(db.Integer, default=1)
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())
    updated_at = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())