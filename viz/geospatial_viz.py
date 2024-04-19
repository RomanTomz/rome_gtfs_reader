import sys
import os

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from data_reader.gtfs_reader import GTFSDataProcessor
from data.gfts_feeds import static_url, realtime_url

import h3
import folium
import matplotlib.pyplot as plt

import pandas as pd


reader = GTFSDataProcessor(static_url=static_url, realtime_url=realtime_url)


def create_hexagon_density_map(df, resolution=8, map_center=[41.9028, 12.4964], zoom_start=11):
    """
    Creates a hexagon density map using H3 and Folium.

    Args:
        df (pandas.DataFrame): DataFrame containing 'stop_lat' and 'stop_lon' columns.
        resolution (int): The H3 resolution to use for the hexagons.
        map_center (list): The latitude and longitude to center the Folium map.
        zoom_start (int): The initial zoom level for the Folium map.

    Returns:
        folium.Map: The Folium map object with the hexagon density layer.
    """

    # Step 1: Convert stop locations to H3 indices
    df['h3_index'] = df.apply(lambda row: h3.geo_to_h3(row['stop_lat'], row['stop_lon'], resolution), axis=1)

    # Step 2: Aggregate stops by H3 index to get densities
    h3_counts = df['h3_index'].value_counts().reset_index()
    h3_counts.columns = ['h3_index', 'count']

    # Step 3: Get hexagon boundaries
    h3_counts['boundary'] = h3_counts['h3_index'].apply(lambda x: h3.h3_to_geo_boundary(x, geo_json=False))

    # Step 4: Plot the hexagons
    m = folium.Map(location=map_center, zoom_start=zoom_start)

    for _, row in h3_counts.iterrows():
        # Determine the color; customize this as needed
        color = '#%02x%02x%02x' % (min(row['count'] * 40, 255), 0, 0)
        folium.Polygon(locations=row['boundary'], weight=2, color=color, fill=True, fill_color=color, fill_opacity=0.4).add_to(m)

    return m

# df = reader.get_stops_location().to_pandas()
# map_obj = create_hexagon_density_map(df)
# map_obj.save('hexagon_map.html')


def plot_departure_times(df):
    """
    Plots the distribution of departure times.

    Args:
        df (pandas.DataFrame): DataFrame containing 'departure_time' column.
    """

    # Step 1: Extract the hour and minute components
    df['hour'] = df['departure_time'].str[:2].astype(int)
    df['minute'] = df['departure_time'].str[3:5].astype(int)

    # Step 2: Calculate the departure time in minutes
    df['departure_minutes'] = df['hour'] * 60 + df['minute']

    # Step 3: Plot the distribution
    ax = df['hour'].plot(kind='hist', bins=24 * 60, figsize=(12, 6), color='skyblue', edgecolor='black')
    ax.set_xlabel('Departure Time (minutes)')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Departure Times')
    ax.grid(axis='y', alpha=0.75)

    plt.tight_layout()
    plt.show()
    
# df_stop_times = reader.get_gtfs_dataframe("stop_times")
# plot_departure_times(df_stop_times)