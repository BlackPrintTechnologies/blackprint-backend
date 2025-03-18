import requests
import geopandas as gpd
from shapely import wkt
from utils.iconUtils import IconMapper


@fused.udf
def udf(bbox: fused.types.TileGDF = None, fid: str = None, radius: int = None):
    """
    Fused.io UDF to fetch brand data from the given API and return a GeoDataFrame.
    
    Args:
        bbox (fused.types.TileGDF): The bounding box (TileGDF) passed to the UDF.
    
    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing brand and geometry information.
    """
    # Define the API endpoint
    api_url = "https://backend.blackprint.ai/brands"
    
    # Prepare the payload for the POST request
    payload = {
        "fid":fid,
        "radius": radius 
    }

    print(f"Payload: {payload}")
    try:
        # Make the POST request to the API
        response = requests.post(api_url, json=payload)
        response.raise_for_status()  # Raise an error for HTTP codes >= 400
        
        # Parse the JSON response
        data = response.json()
        if data.get("message") != "Success":
            raise RuntimeError(f"API returned an error: {data}")
        
        # Extract the response data
        brand_data = data["data"]["response"]
        
        if not brand_data:
            # Return an empty GeoDataFrame
            return gpd.GeoDataFrame(columns=["brand", "geometry"], crs="EPSG:4326")
        
        # Convert the data into a GeoDataFrame
        gdf = gpd.GeoDataFrame(
            [
                {
                    "brand": item["brand"],
                    "main_category" : item['category_1'],
                    #new changes
                    #"icon_url": IconMapper.get_icon_url(item['category_1']),
                    "geometry": wkt.loads(item["geometry_wkt"]),
                }
                for item in brand_data
            ],
            crs="EPSG:4326"  # Ensure WGS 84 CRS
        )
        
        # Ensure valid geometries
        gdf = gdf[gdf.is_valid & ~gdf.is_empty]
        print(gdf)
        # Return the GeoDataFrame
        return gdf
    
    except requests.exceptions.RequestException as e:
        # Handle HTTP request errors
        raise RuntimeError(f"Error calling the API: {str(e)}")
    
    except Exception as e:
        # Handle other errors
        raise RuntimeError(f"Error processing API response: {str(e)}")
