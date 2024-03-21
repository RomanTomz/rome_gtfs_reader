import polars as pl
import requests
import zipfile
import io
from google.transit import gtfs_realtime_pb2

class GTFSDataProcessor:
    def __init__(self, static_url, realtime_url):
        self.static_url = static_url
        self.realtime_url = realtime_url
        self.dtype_spec = {
            'stop_id': pl.Utf8,
            'trip_id': pl.Utf8,
            'route_id': pl.Utf8,
            'shape_id': pl.Utf8,
            'trip_short_name': pl.Utf8,
            # Add other columns and their types as needed
        }
        self.schedule_df = None
        self.realtime_df = None
        self.summary_df = None

    def download_and_extract_static_data(self):
        response = requests.get(self.static_url)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall("temp_gtfs")

    def load_static_data(self):
        self.download_and_extract_static_data()
        stop_times_df = pl.read_csv("temp_gtfs/stop_times.txt", dtypes=self.dtype_spec).lazy()
        trips_df = pl.read_csv("temp_gtfs/trips.txt", dtypes=self.dtype_spec).lazy()
        self.schedule_df = trips_df.join(stop_times_df, on="trip_id")

    def download_and_process_realtime_data(self):
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
        merged_df = self.schedule_df.join(self.realtime_df, on='trip_id')
        group_columns = [col for col in merged_df.columns if col != 'delay']
        self.summary_df = merged_df.group_by(group_columns).agg([
            pl.col('delay').mean().alias('average_delay_minutes')
        ]).collect()

    def get_summary_df(self):
        if self.summary_df is None:
            self.load_static_data()
            self.download_and_process_realtime_data()
            self.merge_and_process_data()
        return self.summary_df

if __name__ == "__main__":
    static_url = "https://dati.comune.roma.it/catalog/dataset/a7dadb4a-66ae-4eff-8ded-a102064702ba/resource/266d82e1-ba53-4510-8a81-370880c4678f/download/rome_static_gtfs_test.zip"
    realtime_url = "https://dati.comune.roma.it/catalog/dataset/a7dadb4a-66ae-4eff-8ded-a102064702ba/resource/bf7577b5-ed26-4f50-a590-38b8ed4d2827/download/rome_trip_updates.pb"
    processor = GTFSDataProcessor(static_url, realtime_url)
    summary_df = processor.get_summary_df()
    print(summary_df)

