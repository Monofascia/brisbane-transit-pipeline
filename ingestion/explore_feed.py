import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import json
import duckdb

# test connection
URL = "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates"
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(requests.get(URL, timeout=30).content)
print(f"Corse nel feed: {len(feed.entity)}")
for entity in feed.entity[:3]:
    # print(json.dumps(MessageToDict(entity), indent=4))
    print(MessageToDict(entity))

# reading test of data/gtfs_static/routes.txt with DUCKDB
duckdb.sql("""
CREATE TABLE routes AS SELECT * FROM read_csv_auto('data/gtfs_static/routes.txt');
SELECT * FROM routes LIMIT 3;""").show() 

duckdb.sql("""SELECT route_short_name, route_long_name, route_type
FROM read_csv_auto('data/gtfs_static/routes.txt')
LIMIT 10;""").show()