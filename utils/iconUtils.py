import boto3
import json

# Load configuration
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

class IconMapper:
    CATEGORY_ICON_MAP = {
        'Active Life': 'active-life.png',
        'Arts & Entertainment': 'arts-entertainment.png',
        'Attractions & Activities': 'attractions.png',
        'Automotive': 'automotive.png',
        'Eat & Drink': 'food-drink.png',
        'Education': 'education.png',
        'Financial Service': 'financial.png',
        'Health & Medical': 'health-medical.png',
        'Public Service & Government': 'government.png',
        'Retail': 'retail.png'
    }

    S3_BUCKET = 'your-public-bucket-name'
    S3_PREFIX = 'icons/'

    @classmethod
    def get_icon_url(cls, category):
        """Get the S3 URL for an icon based on the category"""
        icon_file = cls.CATEGORY_ICON_MAP.get(category, 'default.png')
        return f"https://{cls.S3_BUCKET}.s3.amazonaws.com/{cls.S3_PREFIX}{icon_file}"