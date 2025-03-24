import json

class IconMapper:
    CATEGORY_ICON_MAP = {
        'active_life': 'active+life.svg',
        'arts_and_entertainment': 'arts+and+entertainment.svg',
        'attractions_and_activities': 'attractions+and+activities.svg',
        'automotive': 'automotive.svg',
        'eat_and_drink': 'food.svg',
        'education': 'education.svg',
        'financial_service': 'financial+service.svg',
        'health_and_medical': 'health+and+medical.svg',
        'public_service_and_government': 'public+service+and+gov.svg',
        'retail': 'retail.svg'
    }

    S3_BUCKET = 'blackprint-assets'
    S3_PREFIX = 'pois/'

    @classmethod
    def get_icon_url(cls, category):
        """Get the S3 URL for an icon based on the category"""
        icon_file = cls.CATEGORY_ICON_MAP.get(category, 'default.svg')
        return f"https://{cls.S3_BUCKET}.s3.us-west-1.amazonaws.com/{cls.S3_PREFIX}{icon_file}"

# Example Usage
icon_url = IconMapper.get_icon_url('Active Life')
print(icon_url)
