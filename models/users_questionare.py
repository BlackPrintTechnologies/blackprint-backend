# models/users_questionare.py
from migration_setup import db

class BPUsersQuestionare(db.Model):
    __tablename__ = 'bp_users_questionare'

    bp_user_questionare_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    bp_user_id = db.Column(db.Integer)
    bp_brand_name = db.Column(db.String(100))
    bp_category = db.Column(db.String(100))
    bp_product = db.Column(db.String(100))
    bp_market_segment = db.Column(db.String(100))
    bp_target_audience = db.Column(db.String(100))
    bp_competitor_brands = db.Column(db.ARRAY(db.Text))
    bp_complementary_brands = db.Column(db.ARRAY(db.Text))