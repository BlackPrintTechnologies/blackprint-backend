import requests
import os
import os 
from io import BytesIO

# Load Google API key securely from environment variables
GOOGLE_API_KEY="AIzaSyB7oWwJoNxt1mPMzcLRBMiRKXzzcicPerQ"
# GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

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

def get_street_view_image(pano_id, heading, fov=90, size="600x300"):
    """
    Fetch a Street View image from Google using the pano_id and other parameters.
    """
    image_url = f"https://maps.googleapis.com/maps/api/streetview?size={size}&pano={pano_id}&heading={heading}&fov={fov}&key={GOOGLE_API_KEY}"
    response = requests.get(image_url)
    if response.status_code == 200:
        return BytesIO(response.content)
    return None