from elasticsearch import Elasticsearch
from datetime import datetime

#indexName="brent_spot_price"
fileName="./synthetic_app_generated.log"
headers = {"Content-Type": "application/json"}
es_user=""
es_pwd=""
es_url="http://10.131.206.48:31340"
index_name = "synthetic-app-logs"
# Connect to Elasticsearch
#es = Elasticsearch("http://localhost:9200")  # Change to your host/port
es = Elasticsearch(hosts=[es_url], basic_auth=(es_user,es_pwd), timeout=120 ) #Elasticsearch(es_url)

#index_name = "app-logs"

# Create index if it doesn't exist
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)
#es.indices.create(index=index_name)

# Parse and send logs
with open(fileName, "r") as file:
    for line in file:
        try:
            # Parse line format: "2025-06-20 12:34:56 [INFO] Message..."
            timestamp_str, rest = line.split(" [", 1)
            level, message = rest.split("] ", 1)

            doc = {
                "timestamp": datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S"),
                "level": level.strip(),
                "message": message.strip()
            }

            es.index(index=index_name, document=doc)
        except Exception as e:
            print(f"Failed to process line: {line.strip()} - {e}")
