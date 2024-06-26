import sys
import os

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from datetime import datetime

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
        
    def save_static_feed_files(self):
        """
        Downloads and saves the static GTFS data to the local filesystem.
        """
        target_directory = "temp_gtfs"
        os.makedirs(target_directory, exist_ok=True)
        
        response = requests.get(self.static_url)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                z.extractall(target_directory)
        else:
            raise Exception(f"Failed to download static GTFS data. Status code: {response.status_code}")
        
    @staticmethod
    def extract_file_from_zip(zipfile_obj, filename, dtype_spec):
        """
        Extracts a file from a zipfile object and loads it into a Polars DataFrame.

        Args:
            zipfile_obj (zipfile.ZipFile): The zipfile object.
            filename (str): The name of the file to extract and read.
            dtype_spec (dict): The data type specifications for the columns.

        Returns:
            pl.DataFrame: The extracted file as a Polars DataFrame.
        """
        with zipfile_obj.open(filename) as file:
            return pl.read_csv(file, dtypes=dtype_spec).lazy()


    def load_static_data(self):
        """
        Loads the static GTFS data into a DataFrame directly from the ZIP archive in memory.
        """
        response = requests.get(self.static_url)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            stop_times_df = self.extract_file_from_zip(z, 'stop_times.txt', self.dtype_spec)
            trips_df = self.extract_file_from_zip(z, 'trips.txt', self.dtype_spec)
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

    def merge_and_process_delay_data(self):
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
            self.merge_and_process_delay_data()
        return self.summary_df
    
    def correct_gtfs_times(self, df, time_column):
        """
        Corrects the GTFS time strings in a DataFrame column.

        Args:
            df (pandas.DataFrame): The DataFrame containing GTFS time data.
            time_column (str): The name of the column with time strings to correct.
        """
        # Vectorized operation to correct hours greater than 23
        time_split = df[time_column].str.split(':', expand=True).astype(int)
        time_split[0] = time_split[0] % 24
        df[time_column] = time_split[0].astype(str).str.zfill(2) + ':' + time_split[1].astype(str).str.zfill(2) + ':' + time_split[2].astype(str).str.zfill(2)

    def get_gtfs_dataframe(self, file_name):
        """
        Gets the GTFS DataFrame.

        Args:
            file_name (str): The name of the GTFS file.

        Returns:
            pandas.DataFrame: The GTFS DataFrame containing the static data.
        """
        if self.schedule_df is None:
            self.load_static_data()

        file_path = f"data_reader/rome_static_gtfs/{file_name}.txt"
        if not os.path.exists(file_path):
            self.save_static_feed_files()

        # Ensure that the dtypes are being applied correctly
        df = pl.read_csv(file_path, dtypes=self.dtype_spec).to_pandas()

        # If additional processing is needed for specific files, it can be done here
        if file_name == 'stop_times':
            # Assuming correct_gtfs_times modifies the DataFrame in place
            self.correct_gtfs_times(df, 'arrival_time')
            self.correct_gtfs_times(df, 'departure_time')

        return df
        

if __name__ == "__main__":
    static_url = static_url
    realtime_url = realtime_url
    processor = GTFSDataProcessor(static_url, realtime_url)
    summary_df = processor.get_summary_df()
    print(summary_df)
