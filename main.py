#%%
import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

from find_overlaps import find_overlaps, LatLonBox
import requests
from proto.rocktree_pb2 import NodeData, Texture
from google.protobuf.internal import decoder
import geopandas as gpd
from PIL import Image
import numpy as np
import pandas as pd
import math
import io
import re
import glob
from gpt_tools import analyze_construction_phase_openai, analyze_construction_phase_gemini
import pandas as pd
from shapely.geometry import shape

def download_node_data(octant_path, version_map, year=2024):
    """Download node data for a specific octant and extract textures."""
    base_url = "https://kh.google.com/rt/tm/earth/NodeData/pb="
    
    epoch, version, timestamp = version_map.get(year, (990, 350, 1036419))  # default to 2024
    
    # Format URL similar to the actual Google Earth request
    url = base_url + "!1m2!1s{}!2u{}!2e1!3u{}!4b0!5i{}".format(
        octant_path, epoch, version, timestamp
    )
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Referer': 'https://earth.google.com/'
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return _extract_jpeg_from_protobuf(response.content)
    return None

def _extract_jpeg_from_protobuf(data):
    """Extract JPEG image data from protobuf message"""
    try:
        pos = 0
        while pos < len(data):
            tag, new_pos = decoder._DecodeVarint(data, pos)
            wire_type = tag & 7
            field_number = tag >> 3
            
            if wire_type == 2:  # Length-delimited
                length, content_pos = decoder._DecodeVarint(data, new_pos)
                content = data[content_pos:content_pos + length]
                
                if field_number == 2:
                    # Parse nested message
                    nested_pos = 0
                    while nested_pos < len(content):
                        nested_tag, nested_new_pos = decoder._DecodeVarint(content, nested_pos)
                        nested_wire_type = nested_tag & 7
                        nested_field_number = nested_tag >> 3
                        
                        if nested_wire_type == 2:
                            nested_length, nested_content_pos = decoder._DecodeVarint(content, nested_new_pos)
                            nested_content = content[nested_content_pos:nested_content_pos + nested_length]
                            
                            if nested_field_number == 6:
                                jpeg_data = nested_content[3:]  # Skip the first 3 bytes
                                if jpeg_data.startswith(b'\xFF\xD8'):
                                    return jpeg_data
                            
                            nested_pos = nested_content_pos + nested_length
                        else:
                            nested_pos = nested_new_pos
                
                pos = content_pos + length
            else:
                pos = new_pos
        
        print("No valid JPEG data found in protobuf message")
        return None
    except Exception as e:
        print(f"Error extracting JPEG data: {e}")
        import traceback
        traceback.print_exc()
        return None


def stitch_images(image_dict, octants):
    """
    Stitch images using their geographical positions from octant data
    """
    if not image_dict:
        return None, {"construction_phase": "no_data"}  # Return tuple with None and default analysis
    
    # Create mapping of path to octant object
    octant_map = {octant.path: octant for octant in octants}
    
    # Get dimensions of first image
    first_image = next(iter(image_dict.values()))
    tile_width, tile_height = first_image.size
    
    # Get geographical bounds
    min_lon = min(octant_map[path].bbox.west for path in image_dict.keys())
    max_lon = max(octant_map[path].bbox.east for path in image_dict.keys())
    min_lat = min(octant_map[path].bbox.south for path in image_dict.keys())
    max_lat = max(octant_map[path].bbox.north for path in image_dict.keys())
    
    # Find the size of a single tile in degrees
    sample_octant = next(iter(octant_map.values()))
    lon_step = sample_octant.bbox.east - sample_octant.bbox.west
    lat_step = sample_octant.bbox.north - sample_octant.bbox.south
    
    def get_position(path):
        octant = octant_map[path]
        # Calculate position based on distance from minimum bounds
        x = round((octant.bbox.west - min_lon) / lon_step)
        y = round((max_lat - octant.bbox.north) / lat_step)  # Flip Y axis
        print(f"\nTile {path}:")
        print(f"  Bounds: N:{octant.bbox.north:.6f}, S:{octant.bbox.south:.6f}, "
              f"E:{octant.bbox.east:.6f}, W:{octant.bbox.west:.6f}")
        print(f"  Position: ({x}, {y})")
        return x, y
    
    # Calculate grid size
    positions = [get_position(path) for path in image_dict.keys()]
    max_x = max(x for x, _ in positions) + 1
    max_y = max(y for _, y in positions) + 1
    
    print(f"\nGrid size: {max_x}x{max_y}")
    
    # Create canvas
    final_image = Image.new('RGB', (max_x * tile_width, max_y * tile_height))
    
    # Place images
    for path, img in sorted(image_dict.items()):
        x, y = get_position(path)
        pixel_x = x * tile_width
        pixel_y = y * tile_height
        final_image.paste(img, (pixel_x, pixel_y))
        print(f"Placed tile {path} at position ({x}, {y})")
    
    # Save final image to temporary file for analysis
    temp_filename = 'temp_combined.jpg'
    final_image.save(temp_filename)
    
    # Analyze construction phase
    analysis = analyze_construction_phase_openai(temp_filename)
    os.remove(temp_filename)  # Clean up temporary file
    
    return final_image, analysis

def extract_mapping(pb):
    # Match patterns for epoch, version, and timestamp
    epoch_match = re.search(r'2u(\d+)', pb)
    version_match = re.search(r'3u(\d+)', pb)
    timestamp_match = re.search(r'5i(\d+)', pb)

    # Extract numeric values
    epoch = int(epoch_match.group(1)) if epoch_match else None
    version = int(version_match.group(1)) if version_match else None
    timestamp = int(timestamp_match.group(1)) if timestamp_match else None

    return epoch, version, timestamp

# Test examples
pb_2024 = "!m2!s30524153625370535063!2u990!2e1!3u350!4b0!5i1036419"
pb_2019 = "!m2!s30524153625370535241!2u990!2e1!3u253!4b0!5i1033769"
pb_2016 = "!m2!s30524153625371424241!2u990!2e1!3u215!4b1!5i1032357"
pb_2015 = "!m2!s30524153625370535163!2u990!2e1!3u215!4b1!5i1031986"
pb_2022 = "!m2!s30524153625370535341!2u990!2e1!3u342!4b0!5i1035410"

# Map years to their corresponding versions and timestamps
version_map = {
    # 2015: extract_mapping(pb_2015),
    # 2016: extract_mapping(pb_2016),
    2019: extract_mapping(pb_2019),
    # 2022: extract_mapping(pb_2022),
    2024: extract_mapping(pb_2024),
}

# load geojson
gdf = gpd.read_file('data/dubai_buildings_gt_50k_sqft.geojson')

# load first 20 for testing
gdf = gdf.iloc[300:600]

# Create list to store results
results = []

# Process each AOI
for idx, aoi in gdf.iterrows():
    print(f"\nProcessing AOI {idx + 1}/{len(gdf)}")
    
    # Create bbox from the geojson feature
    bounds = aoi.geometry.bounds
    bbox = LatLonBox(
        north=bounds[3],
        south=bounds[1],
        west=bounds[0],
        east=bounds[2]
    )
    
    # Create maps URL from centroid
    centroid = aoi.geometry.centroid
    maps_url = f"https://maps.google.com/?q={centroid.y},{centroid.x}"
    
    print(f"Bounding box: {bbox}")
    
    # Get overlapping octants
    overlapping_octants = find_overlaps(bbox, 200)
    
    # Create dictionaries to store images by year
    images = {year: {} for year in version_map.keys()}
    octants_list = []
    
    level = 20
    # Process the results and download data
    if level in overlapping_octants:
        print(f"[Octant level {level}]")
        for octant in overlapping_octants[level]:
            octants_list.append(octant)
            print(octant.path)
            
            # Download for each year
            for year in version_map.keys():
                print(f"Downloading for year {year}")
                jpeg_data = download_node_data(
                    octant.path,
                    version_map,
                    year=year
                )
                       
                if jpeg_data:
                    images[year][octant.path] = Image.open(io.BytesIO(jpeg_data))
                    print(f"Downloaded tile for {year}: {octant.path}")
    
    # Stitch and save final images for this AOI
    for year in version_map.keys():
        final_image, analysis = stitch_images(images[year], octants_list)
        if final_image:
            filename = f'images/aoi_{idx+1}_{year}_{analysis["construction_phase"]}.jpg'
            final_image.save(filename)
            print(f"Saved combined image for AOI {idx+1}, year {year}: {filename}")
            
            # Store result
            results.append({
                'aoi_number': idx + 1,
                'year': year,
                'construction_status': analysis["construction_phase"],
                'confidence_level': analysis["confidence_level"],
                'reasoning': analysis["reasoning"],
                'maps_url': maps_url
            })

    # Save results after each AOI (in case of crashes)
    df = pd.DataFrame(results)
    df.to_csv('construction_analysis.csv', index=False)
    print(f"Updated results saved to construction_analysis.csv")

# Final summary
distressed_aois = df[df['construction_status'] == 'CONSTRUCTION'].groupby('aoi_number').filter(lambda x: len(x) > 1)['aoi_number'].unique()
if len(distressed_aois) > 0:
    print(f"\nFound {len(distressed_aois)} potentially distressed AOIs: {distressed_aois}")
else:
    print("\nNo potentially distressed AOIs found")
# %%

# load the csv
df = pd.read_csv('construction_analysis.csv')

# Filter for different construction statuses in 2019 and 2024
df_2019_construction = df[(df['year'] == 2019) & (df['construction_status'] == 'CONSTRUCTION')]['aoi_number']
df_2019_groundworks = df[(df['year'] == 2019) & (df['construction_status'] == 'GROUNDWORKS')]['aoi_number']
df_2024_construction = df[(df['year'] == 2024) & (df['construction_status'] == 'CONSTRUCTION')]['aoi_number']
df_2024_groundworks = df[(df['year'] == 2024) & (df['construction_status'] == 'GROUNDWORKS')]['aoi_number']

# Find stalled projects in different categories
stalled_construction = set(df_2019_construction).intersection(set(df_2024_construction))
stalled_groundworks = set(df_2019_groundworks).intersection(set(df_2024_groundworks))
groundworks_to_construction = set(df_2019_groundworks).intersection(set(df_2024_construction))
construction_to_groundworks = set(df_2019_construction).intersection(set(df_2024_groundworks))

# Filter and save separate CSVs for each category
if stalled_construction:
    construction_df = df[df['aoi_number'].isin(stalled_construction) & (df['year'] == 2024)]
    construction_df['stall_type'] = 'Construction-to-Construction'
    construction_df.to_csv('stalled_construction_to_construction.csv', index=False)

if stalled_groundworks:
    groundworks_df = df[df['aoi_number'].isin(stalled_groundworks) & (df['year'] == 2024)]
    groundworks_df['stall_type'] = 'Groundworks-to-Groundworks'
    groundworks_df.to_csv('stalled_groundworks_to_groundworks.csv', index=False)

if groundworks_to_construction:
    groundworks_to_const_df = df[df['aoi_number'].isin(groundworks_to_construction) & (df['year'] == 2024)]
    groundworks_to_const_df['stall_type'] = 'Groundworks-to-Construction'
    groundworks_to_const_df.to_csv('stalled_groundworks_to_construction.csv', index=False)

if construction_to_groundworks:
    const_to_groundworks_df = df[df['aoi_number'].isin(construction_to_groundworks) & (df['year'] == 2024)]
    const_to_groundworks_df['stall_type'] = 'Construction-to-Groundworks'
    const_to_groundworks_df.to_csv('stalled_construction_to_groundworks.csv', index=False)

# Also save a combined CSV with all stalled projects
all_stalled_aois = stalled_construction.union(stalled_groundworks).union(groundworks_to_construction).union(construction_to_groundworks)
stalled_df = df[df['aoi_number'].isin(all_stalled_aois) & (df['year'] == 2024)]
stalled_df['stall_type'] = stalled_df.apply(lambda row: 
    'Construction-to-Construction' if row['aoi_number'] in stalled_construction
    else 'Groundworks-to-Groundworks' if row['aoi_number'] in stalled_groundworks
    else 'Groundworks-to-Construction' if row['aoi_number'] in groundworks_to_construction
    else 'Construction-to-Groundworks', axis=1)
stalled_df.to_csv('all_stalled_projects.csv', index=False)

print("\nAOIs with stalled development:")
print("Construction-to-Construction:", sorted(list(stalled_construction)))
print("Groundworks-to-Groundworks:", sorted(list(stalled_groundworks)))
print("Groundworks-to-Construction:", sorted(list(groundworks_to_construction)))
print("Construction-to-Groundworks:", sorted(list(construction_to_groundworks)))

# %%
