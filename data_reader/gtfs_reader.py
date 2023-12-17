import polars as pl
import zipfile
import requests
from google.transit import gtfs_realtime_pb2

# URLs for the GTFS static data and the GTFS realtime updates
static_url = "https://dati.comune.roma.it/catalog/dataset/a7dadb4a-66ae-4eff-8ded-a102064702ba/resource/266d82e1-ba53-4510-8a81-370880c4678f/download/rome_static_gtfs_test.zip"
realtime_url = "https://dati.comune.roma.it/catalog/dataset/a7dadb4a-66ae-4eff-8ded-a102064702ba/resource/bf7577b5-ed26-4f50-a590-38b8ed4d2827/download/rome_trip_updates.pb"

# Define the data types explicitly
dtype_spec = {
    'stop_id': pl.Utf8,
    'trip_id': pl.Utf8,
    'route_id': pl.Utf8,
    'shape_id': pl.Utf8,
    'trip_short_name': pl.Utf8,
    # Add other columns and their types as needed
}

# Load schedules from GTFS static data using Polars
stop_times_df = pl.read_csv("rome_static_gtfs/stop_times.txt", dtypes=dtype_spec).lazy()
trips_df = pl.read_csv("rome_static_gtfs/trips.txt", dtypes=dtype_spec).lazy()

# Combine schedule data into one DataFrame using Polars
schedule_df = trips_df.join(stop_times_df, on="trip_id")

# Download and parse GTFS realtime data
realtime_response = requests.get(realtime_url)
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(realtime_response.content)

# Collect real-time updates using Polars
realtime_updates = []
for entity in feed.entity:
    if entity.HasField('trip_update'):
        trip_id = entity.trip_update.trip.trip_id
        for update in entity.trip_update.stop_time_update:
            if update.HasField('arrival') and update.arrival.delay > 0:
                delay = update.arrival.delay
                realtime_updates.append({'trip_id': trip_id, 'delay': delay})

# Convert to DataFrame using Polars
realtime_df = pl.DataFrame(realtime_updates).lazy()

# Merge with static data to get a complete view using Polars
merged_df = schedule_df.join(realtime_df, on='trip_id')

# Create summary - Example: Average delay per route using Polars
summary_df = merged_df.groupby('route_id').agg(pl.col('delay').mean())

# Collect the results and print or save the summary DataFrame
summary_df = summary_df.collect()
print(summary_df)
