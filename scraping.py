import requests
import csv
from collections import defaultdict
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
url = "https://rtwqmsdb1.cpcb.gov.in/data/internet/layers/10/index.json"
response = requests.get(url, verify=False)
data = response.json()

grouped_data = defaultdict(dict)
for item in data:
   station = item.get("station_name", item.get("station_id"))
   timestamp = item["timestamp"]
   parameter = item["stationparameter_longname"]
   value = item["ts_value"]
   key = (station, timestamp)
   grouped_data[key]["Station"] = station
   grouped_data[key]["Timestamp"] = timestamp
   grouped_data[key][parameter] = value

all_parameters = set()
for record in grouped_data.values():
   all_parameters.update(record.keys())

columns = ["Station", "Timestamp"] + sorted([p for p in all_parameters if p not in ("Station", "Timestamp")])
filename = "parameter_pivoted.csv"
file_exists = os.path.isfile(filename)

with open(filename, mode="a", newline="", encoding="utf-8") as file:
   writer = csv.DictWriter(file, fieldnames=columns)
   if not file_exists:
       writer.writeheader()
   for record in grouped_data.values():
       writer.writerow(record)
print(f"data appended to {filename}")