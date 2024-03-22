import sys
import os

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from data_reader.gtfs_reader import GTFSDataProcessor
from data.gfts_feeds import static_url, realtime_url

reader = GTFSDataProcessor(static_url=static_url, realtime_url=realtime_url)

summary_df = reader.get_summary_df()

print(summary_df.columns)