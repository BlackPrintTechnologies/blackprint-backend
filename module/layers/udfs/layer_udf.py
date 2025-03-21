from sklearn.cluster import DBSCAN
import numpy as np
import requests
import traceback
import geopandas as gpd
import fused
from shapely.geometry import Point
import json

def cluster_points(gdf, eps=0.01, min_samples=5):
    if gdf.empty:
        return gdf

    coords = np.array([[geom.x, geom.y] for geom in gdf.geometry])
    
    # Apply DBSCAN clustering
    clustering = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean').fit(coords)
    
    gdf['cluster_id'] = clustering.labels_
    
    # Aggregate clusters
    cluster_gdf = gdf.dissolve(by='cluster_id', aggfunc='count').reset_index()
    cluster_gdf['geometry'] = gdf.groupby('cluster_id')['geometry'].apply(lambda x: x.unary_union.centroid)
    
    return cluster_gdf

@fused.udf
def udf(bbox: fused.types.TileGDF = None, fid: str = None, radius: int = None):
    api_url = "http://staging.blackprint.ai/property/layer"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        
        if data.get("message") != "Success":
            raise RuntimeError(f"API returned an error: {data}")
        
        property_data = data["data"]["response"]

        if not property_data:
            return gpd.GeoDataFrame(columns=["centroid", "geometry"], crs="EPSG:4326")

        def get_point(centroid):
            return Point(json.loads(centroid)['coordinates'][0], json.loads(centroid)['coordinates'][1])
        
        gdf = gpd.GeoDataFrame(
            [
                {
                    "centroid": item["centroid"],
                    "floor_levels": item["floor_levels"],
                    "id_city_blocks": item["id_city_blocks"],
                    "id_land_use": item["id_land_use"],
                    "id_municipality": item["id_municipality"],
                    "street_address": item['street_address'],
                    "geometry": get_point(item['centroid']),
                }
                for item in property_data
            ],
            crs="EPSG:4326"
        )

        gdf = gdf[gdf.is_valid & ~gdf.is_empty]
        

        return gdf
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error calling the API: {str(e)}")
    
    except Exception as e:
        print(traceback.format_exc())
        raise RuntimeError(f"Error processing API response: {str(e)}")
