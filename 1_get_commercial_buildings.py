#%%
import geopandas as gpd
from shapely import wkb, wkt
import duckdb
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

duckdb_folder_path = "data"

#%%

# Check if the folder exists; if not, create it
if not os.path.exists(duckdb_folder_path):
    os.makedirs(duckdb_folder_path)
    print(f"Folder created at: {os.path.abspath(duckdb_folder_path)}")
else:
    print(f"Folder already exists at: {os.path.abspath(duckdb_folder_path)}")

# Initialize DuckDB connection
con = duckdb.connect(f'{duckdb_folder_path}/overture_data.db')

# Load required extensions
con.execute("INSTALL spatial;")
con.execute("INSTALL httpfs;")
con.execute("LOAD spatial;")
con.execute("SET s3_region='us-west-2';")

logging.info("DuckDB connection and extensions set up successfully.")

def get_overture_data(polygon_wkt, theme, type):
    """
    Fetches Overture data based on a spatial polygon.

    Parameters:
    - polygon_wkt (str): The Well-Known Text representation of the polygon.
    - theme (str): The theme to filter (e.g., "buildings").
    - type (str): The type within the theme (e.g., "building").

    Returns:
    - geopandas.GeoDataFrame: The resulting GeoDataFrame with the filtered data.
    """
    # Get the bounds of the polygon for bbox filtering
    polygon = wkt.loads(polygon_wkt)
    bounds = polygon.bounds  # Returns (minx, miny, maxx, maxy)
    
    logging.info(f"Fetching Overture data for {theme}/{type}")
    theme_type = f'theme={theme}/type={type}/*'
    select_fields = """
        *,
        ST_AsWKB(geometry) as geometry_wkb
    """
    
    query = f"""
    SELECT
        {select_fields}
    FROM read_parquet('s3://overturemaps-us-west-2/release/2024-08-20.0/{theme_type}', filename=true, hive_partitioning=1)
    WHERE bbox.xmin <= {bounds[2]} AND bbox.xmax >= {bounds[0]}
    AND bbox.ymin <= {bounds[3]} AND bbox.ymax >= {bounds[1]}
    """
    result = con.execute(query).df()
    logging.info(f"Fetched {len(result)} {theme}/{type} records")
    return result

dubai_polygon_gdf = gpd.read_file("areas_of_interest.geojson")
DUBAI_POLYGON_WKT = dubai_polygon_gdf.geometry.iloc[0].wkt

# Extract and save raw building data within the polygon
buildings_df = get_overture_data(DUBAI_POLYGON_WKT, theme="buildings", type="building")
buildings_df['geometry'] = buildings_df['geometry_wkb'].apply(lambda x: wkb.loads(bytes(x)))

# Drop unused columns
buildings_df = buildings_df.drop(columns=['geometry_wkb','bbox', 'filename', 'theme', 'type'])
buildings_gdf = gpd.GeoDataFrame(buildings_df, geometry='geometry', crs='EPSG:4326')

# Save GeoDataFrame to GeoJSON
output_geojson = "data/dubai_buildings_raw.geojson"
buildings_gdf.to_file(output_geojson, driver="GeoJSON")
logging.info(f"Saved raw buildings data to {output_geojson}")

#%%

# Continue with your existing processing steps...
# For example: Filter commercial buildings, add buffers, etc.
output_geojson = "data/dubai_buildings_raw.geojson"

# Example: Filter commercial buildings (over 50,000 sqft)
gdf = gpd.read_file(output_geojson)
# Reproject to a projected CRS (e.g., EPSG:3857 for accurate area calculation)
gdf = gdf.to_crs(epsg=3857)

# Calculate the area in square feet (1 m² = 10.7639 ft²)
gdf['area_sqft'] = gdf['geometry'].area * 10.7639

# Filter buildings with area greater than 50,000 square feet
filtered_gdf = gdf[gdf['area_sqft'] > 12000]

# set the CRS back to 4326
filtered_gdf = filtered_gdf.to_crs(epsg=4326)

# Save the filtered GeoDataFrame to a new GeoJSON file (optional)
output_filtered_geojson = "data/dubai_buildings_gt_12k_sqft.geojson"
filtered_gdf.to_file(output_filtered_geojson, driver='GeoJSON')
logging.info(f"Saved filtered buildings data to {output_filtered_geojson}")

# Display the filtered GeoDataFrame
print(filtered_gdf.head())



# %%
