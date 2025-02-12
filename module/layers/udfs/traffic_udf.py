import requests
import geopandas as gpd
import pandas as pd
import psycopg2
import h3
from shapely.geometry import Polygon

def get_traffic_query(catchment, fid):
    """Generates SQL query based on catchment radius and fid."""
    query_map = {
        '500': f'''SELECT *
                    FROM blackprint_db_prd.presentation.dataset_mobility_data_h3 where fid={fid} and type='CIRCLE_500_METERS' ''',
        '1000': f'''SELECT *
                    FROM blackprint_db_prd.presentation.dataset_mobility_data_h3 where fid={fid} and type='CIRCLE_1000_METERS' ''',
        '5': f'''SELECT *
                    FROM blackprint_db_prd.presentation.dataset_mobility_data_h3 where fid={fid} and type='FRONT_OF_STORE' '''
    }
    return query_map.get(catchment)

@fused.udf
def udf(bbox: fused.types.TileGDF = None, fid: str = None, radius: str = None):
    """
    Fused.io UDF to fetch traffic data and return a GeoDataFrame with H3 hexagon boundaries as polygons.
    
    Args:
        bbox (fused.types.TileGDF): Bounding box (TileGDF).
        fid (str): Feature ID for filtering.
        radius (str): Radius for catchment query ('500', '1000', '5').
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame with H3 hexagon boundaries as polygons.
    """
    config = {
        'host': "blackprint-cluster-prd.czdk80krdpeg.us-west-1.redshift.amazonaws.com",
        'database': 'blackprint_db_prd',
        'user': "admin_user",
        'password': "dee0jsdeERSpO5658sfiejde#asnf$?",
        'port': "5439"
    }
    # api_url = "https://backend.blackprint.ai/traffic"
    
    # # Prepare the payload for the POST request
    # payload = {
    #     "fid":fid,
    #     "radius": radius 
    # }
    try:
        # Establish database connection
        connection = psycopg2.connect(**config)
        
        # Use server-side cursor for efficient fetching
        cursor = connection.cursor(name='server_cursor')
        
        # Execute the query
        query = get_traffic_query(radius, fid)
        if not query:
            raise ValueError(f"Invalid radius provided: {radius}")
        
        print(f"Executing query: {query}")
        cursor.execute(query)
        
        # Fetch results in batches
        batch_size = 1000
        rows = []
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            rows.extend(batch)
            print(f"Fetched batch of {len(batch)} rows")

        # Extract column names
        col_names = [desc[0] for desc in cursor.description]
        print(f"Columns fetched: {col_names}")
        df = pd.DataFrame(rows, columns=col_names)

        # Close the cursor
        cursor.close()

        # Convert H3 indices to boundary polygons
        if 'h3_index' not in df.columns:
            raise ValueError("Expected 'h3_index' column in the query result.")
        
        # def h3_to_polygon(h3_index):
        #     try:
        #         hex_boundary = h3.cell_to_boundary(h3.int_to_str(h3_index))
        #         # Reverse each coordinate pair to (lng, lat)
        #         hex_boundary_lnglat = [(lng, lat) for lat, lng in hex_boundary]
        #         return Polygon(hex_boundary_lnglat)
        #     except Exception as e:
        #         print(f"Error converting H3 index {h3_index} to polygon: {e}")
        #         return None

        df['geometry'] = df['h3_index'].apply(lambda h3_index: Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(h3.int_to_str(h3_index))]))

        # df['geometry'] = df['h3_index'].apply(h3_to_polygon)

        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
        gdf = gdf[gdf.is_valid & ~gdf.is_empty]

        print(f"GeoDataFrame created with {len(gdf)} rows")
        print(gdf['geometry'].iloc[0])  # Inspect the first geometry
        return gdf

    except psycopg2.Error as e:
        print(f"Database error: {str(e)}")
        raise RuntimeError(f"Database error: {str(e)}")
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise RuntimeError(f"Error processing data: {str(e)}")
    finally:
        if 'connection' in locals():
            connection.close()
