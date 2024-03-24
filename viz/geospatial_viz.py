import sys
import os

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from data_reader.gtfs_reader import GTFSDataProcessor
from data.gfts_feeds import static_url, realtime_url

import h3
import folium

import pandas as pd


reader = GTFSDataProcessor(static_url=static_url, realtime_url=realtime_url)

summary_df = reader.get_summary_df()

print(summary_df.columns)

df = reader.get_stops_location()
df = df.to_pandas()

import h3
import folium

resolution = 8

# Using apply with axis=1 to apply the function row-wise
df['h3_index'] = df.apply(lambda row: h3.geo_to_h3(row['stop_lat'], row['stop_lon'], resolution), axis=1)

# Step 2: Aggregate stops by H3 index to get densities
h3_counts = df['h3_index'].value_counts().reset_index()

h3_counts.columns = ['h3_index', 'count']

# Step 3: Get hexagon boundaries
h3_counts['boundary'] = h3_counts['h3_index'].apply(lambda x: h3.h3_to_geo_boundary(x, geo_json=False))

# Step 4: Plot the hexagons
# Initialize a Folium map at a central location
m = folium.Map(location=[41.9028, 12.4964], zoom_start=11)

# Add hexagons to the map
for _, row in h3_counts.iterrows():
    # Use the count to determine the color, you can customize this part
    color = '#%02x%02x%02x' % (min(row['count'] * 40, 255), 0, 0)
    
    # Create the polygon for the hexagon
    folium.Polygon(locations=row['boundary'], weight=2, color=color, fill=True, fill_color=color, fill_opacity=0.4).add_to(m)

# Save or display the map
m.save('hexagon_map.html')
