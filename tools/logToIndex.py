from elasticsearch import Elasticsearch
from datetime import datetime
import logging
import requests
import json

logging.basicConfig(level=logging.DEBUG)

#indexName="brent_spot_price"
fileName="/home/bhupinder_parmar/es/es-tools/tools/synthetic_app_generated.log"
headers = {"Accept": "application/vnd.elasticsearch+json; compatible-with=8", "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"}
es_user=""
es_pwd=""
es_url="http://10.131.206.48:31340"
index_name = "synthetic-app-logs"
# Connect to Elasticsearch
#es = Elasticsearch("http://10.131.206.48:31340", headers={
#  "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
#  "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"
#})  # Change to your host/port
#es = Elasticsearch(hosts=[es_url], verify_certs=False, basic_auth=(es_user,es_pwd), request_timeout=120 ) #Elasticsearch(es_url)

#index_name = "app-logs"

# Create index if it doesn't exist
#idx =  es.indices.exists(index=index_name)
#if (idx is None) or (not idx):
#    es.indices.create(index=index_name)
#es.indices.create(index=index_name)
# Only once
#response = requests.put(f"{es_url}/{index_name}", headers=headers)
#print(response.status_code, response.text)
# Parse and send logs
ex=False
with open(fileName, "r") as file:
    for line in file:
        try:
            # Parse line format: "2025-06-20 12:34:56 [INFO] Message..."
            timestamp_str, rest = line.split(" [", 1)
            level, message = rest.split("] ", 1)

            doc = {
                "timestamp": datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").isoformat(),
                "level": level.strip(),
                "message": message.strip()
            }

            # es.index(index=index_name, document=doc)
            response = requests.post(f"{es_url}/{index_name}/_doc", headers=headers, json=doc)
            print(response.status_code, response.json())
        except Exception as e:
            print(f"Failed to process line: {line.strip()} - {e}")
            ex=True
            break
        if ex: break

