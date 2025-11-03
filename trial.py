from shapely import wkt
from shapely.ops import transform
from pyproj import CRS, Transformer

# Your polygon WKT
polygon_wkt = """POLYGON ((2473514.2858999968 1103271.6464999989, 2473536.692900002 1103288.9857, 2473613.4888999984 1103181.7608999982, 2473591.639899999 1103166.0788999982, 2473514.2858999968 1103271.6464999989))"""

# Parse WKT
polygon = wkt.loads(polygon_wkt)

# Define candidate SRIDs (likely in meters)
candidate_srid_list = [32643, 32644, 3857]  # UTM Zones 43N, 44N, Web Mercator

for srid in candidate_srid_list:
    src_crs = CRS.from_epsg(srid)
    transformer = Transformer.from_crs(src_crs, 4326, always_xy=True)
    transformed = transform(transformer.transform, polygon)
    lon, lat = transformed.centroid.x, transformed.centroid.y
    print(f"SRID {srid} => centroid at (lat={lat:.5f}, lon={lon:.5f})")

print("\nTry whichever centroid falls inside your expected region.")
