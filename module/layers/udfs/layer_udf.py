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
def udf(bbox: fused.types.TileGDF = None, fid: str = None, radius: int = None, city: str = 'mexico'):
    # Make API call city-aware
    if city == 'queretaro' or city == 'el_marques':
        api_url = "http://staging.blackprint.ai/property/layer?config_city=queretaro"
    else:
        api_url = "http://staging.blackprint.ai/property/layer?config_city=mexico"
    
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
            if centroid:
                return Point(json.loads(centroid)['coordinates'][0], json.loads(centroid)['coordinates'][1])
            return None
        
        # Handle different column structures for different cities
        if city == 'queretaro' or city == 'el_marques':
            gdf = gpd.GeoDataFrame(
                [
                    {
                        "fid": item["fid"],
                        "centroid": item["centroid"],
                        "is_on_market": item.get("is_on_market"),
                        "ids_market_data_spot2": item.get("ids_market_data_spot2"),
                        "ids_market_data_inmuebles24": item.get("ids_market_data_inmuebles24"),
                        "geometry_type": item.get("geometry_type"),
                        "bbox": item.get("bbox"),
                        "predominant_level": item.get("predominant_level"),
                        "total_houses": item.get("total_houses"),
                        "id_municipality": item.get("id_municipality"),
                        "geometry": get_point(item.get('centroid')),
                    }
                    for item in property_data if item.get('centroid')
                ],
                crs="EPSG:4326"
            )
        else:
            # Mexico structure (original)
            gdf = gpd.GeoDataFrame(
                [
                    {
                        "fid": item["fid"],
                        "centroid": item["centroid"],
                        "floor_levels": item.get("floor_levels"),
                        "id_city_blocks": item.get("id_city_blocks"),
                        "id_land_use": item.get("id_land_use"),
                        "id_municipality": item.get("id_municipality"),
                        "street_address": item.get('street_address'),
                        "is_on_market": item.get("is_on_market"),
                        "total_surface_area": item.get("total_surface_area"),
                        "total_construction_area": item.get("total_construction_area"),
                        "geometry": get_point(item.get('centroid')),
                    }
                    for item in property_data if item.get('centroid')
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
