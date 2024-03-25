import sys
import os

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import polars as pl
import requests
import zipfile
import io
from google.transit import gtfs_realtime_pb2
from data.gfts_feeds import static_url, realtime_url

class GTFSDataProcessor:
    def __init__(self, static_url, realtime_url):
        """
        Initializes an instance of GTFSDataProcessor.

        Args:
            static_url (str): The URL to download the static GTFS data.
            realtime_url (str): The URL to download the real-time GTFS data.
        """
        self.static_url = static_url
        self.realtime_url = realtime_url
        self.dtype_spec = {
            'stop_id': pl.Utf8,
            'trip_id': pl.Utf8,
            'route_id': pl.Utf8,
            'shape_id': pl.Utf8,
            'trip_short_name': pl.Utf8,
            'service_id': pl.Utf8,
            'direction_id': pl.Int32,
            'stop_sequence': pl.Int32,
            'arrival_time': pl.Utf8,
            'departure_time': pl.Utf8,
            'stop_headsign': pl.Utf8,
            'pickup_type': pl.Int32,
            'drop_off_type': pl.Int32,
            'timepoint': pl.Int32,
            'delay': pl.Int32,
            'stop_lat': pl.Float64,
            'stop_lon': pl.Float64,
            'stop_code': pl.Utf8,
            
            
            # Add other columns and their types as needed
        }
        self.schedule_df = None
        self.realtime_df = None
        self.summary_df = None

    def download_and_extract_static_data(self):
        """
        Downloads and extracts the static GTFS data from the provided URL.
        """
        response = requests.get(self.static_url)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall("temp_gtfs")

    def load_static_data(self):
        """
        Loads the static GTFS data into a DataFrame.
        """
        self.download_and_extract_static_data()
        stop_times_df = pl.read_csv("temp_gtfs/stop_times.txt", dtypes=self.dtype_spec).lazy()
        trips_df = pl.read_csv("temp_gtfs/trips.txt", dtypes=self.dtype_spec).lazy()
        self.schedule_df = trips_df.join(stop_times_df, on="trip_id")
        
    def get_stops_location(self):
        """
        Gets the location of the stops from the static GTFS data.

        Returns:
            pl.DataFrame: The DataFrame containing the stop_id and the location of the stops.
        """
        if self.schedule_df is None:
            self.load_static_data()
        stops_df = pl.read_csv("temp_gtfs/stops.txt", dtypes=self.dtype_spec).lazy()
        return stops_df.select(['stop_id', 'stop_lat', 'stop_lon']).collect()

    def download_and_process_delay_data(self):
        """
        Downloads and processes the real-time GTFS data from the provided URL.
        """
        realtime_response = requests.get(self.realtime_url)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(realtime_response.content)
        realtime_updates = []
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                trip_id = entity.trip_update.trip.trip_id
                for update in entity.trip_update.stop_time_update:
                    if update.HasField('arrival') and update.arrival.delay > 0:
                        delay_in_minutes = update.arrival.delay / 60  # Convert delay to minutes
                        realtime_updates.append({'trip_id': trip_id, 'delay': delay_in_minutes})
        self.realtime_df = pl.DataFrame(realtime_updates).lazy()

    def merge_and_process_data(self):
        """
        Merges and processes the static and real-time GTFS data.
        """
        merged_df = self.schedule_df.join(self.realtime_df, on='trip_id')
        group_columns = [col for col in merged_df.columns if col != 'delay']
        self.summary_df = (
            merged_df
            .group_by(group_columns)
            .agg([
            pl.col('delay').mean().alias('average_delay_minutes')
        ])).unique(subset=['route_id', 'service_id','trip_id']).collect()

    def get_summary_df(self):
        """
        Gets the summary DataFrame.

        Returns:
            pl.DataFrame: The summary DataFrame containing the average delay for each route, service, and trip.
        """
        if self.summary_df is None:
            self.load_static_data()
            self.download_and_process_delay_data()
            self.merge_and_process_data()
        return self.summary_df

if __name__ == "__main__":
    static_url = static_url
    realtime_url = realtime_url
    processor = GTFSDataProcessor(static_url, realtime_url)
    summary_df = processor.get_summary_df()
    print(summary_df)
