import requests
import os
import os 
from io import BytesIO
import json
from functools import lru_cache, wraps
import logging

logger = logging.getLogger(__name__)

# Load Google API key securely from app.json
# Load configuration from app.json
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)
    
GOOGLE_API_KEY = config["GOOGLE_API_KEY"]

# Import Redis cache
try:
    from utils.redis_cache import redis_cache, generate_cache_key
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis cache not available for street view")

def get_street_view_metadata(lat, lng):
    """
    Fetch the pano_id from Google Street View metadata.
    """
    metadata_url = f"https://maps.googleapis.com/maps/api/streetview/metadata?location={lat},{lng}&key={GOOGLE_API_KEY}"
    response = requests.get(metadata_url)
    data = response.json()
    if data.get("status") == "OK":
        return data["pano_id"]
    return None

def get_street_view_metadata_cached(lat, lng):
    if not REDIS_AVAILABLE:
        return get_street_view_metadata(lat, lng)
    
    # Generate cache key for street view metadata
    cache_key = generate_cache_key("street_view_metadata", lat, lng)
    logger.info(f"🔑 Generated cache key: {cache_key} for coordinates ({lat}, {lng})")
    
    # Try to get from cache
    cached_pano_id = redis_cache.get(cache_key)
    if cached_pano_id:
        logger.info(f"✅ CACHE HIT for street view metadata: {cache_key}")
        return cached_pano_id
    
    # Get from Google API
    logger.info(f"🔄 API CALL for street view metadata: ({lat}, {lng})")
    pano_id = get_street_view_metadata(lat, lng)
    
    # Cache only valid pano_ids
    if pano_id:
        redis_cache.set(cache_key, pano_id, ttl=604800)  # 7 days
        logger.info(f"💾 CACHE SET for street view metadata: {cache_key}")
    else:
        logger.info(f"❌ No pano_id found for coordinates ({lat}, {lng})")
    
    return pano_id

def get_street_view_image(pano_id, heading, fov=90, size="600x300"):
    """
    Fetch a Street View image from Google using the pano_id and other parameters.
    """
    image_url = f"https://maps.googleapis.com/maps/api/streetview?size={size}&pano={pano_id}&heading={heading}&fov={fov}&key={GOOGLE_API_KEY}"
    response = requests.get(image_url)
    if response.status_code == 200:
        return BytesIO(response.content)
    return None