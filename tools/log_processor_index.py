from elasticsearch import Elasticsearch
import elasticsearch
from datetime import datetime
import re

# indexName="brent_spot_price"
fileName = "./synthetic_app_generated.log"
headers = {"Content-Type": "application/json"}
es_user = ""
es_pwd = ""
es_url = "http://10.131.206.48:31340"
index_name = "synthetic-app-logs-2"
headers = {"Accept":"application/vnd.elasticsearch+json; compatible-with=8", "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"}

# Connect to Elasticsearch
es = Elasticsearch(es_url, headers=headers)  # Change to your host/port
# es = Elasticsearch(hosts=[es_url] , basic_auth=(es_user, es_pwd), timeout=120)  # Elasticsearch(es_url)

# Create index if it doesn't exist
# es = es.options(headers=headers)
# print(es.indices.get(index=index_name))
idx = es.indices.exists(index=index_name)
# # try:
# #     es.indices.get(index=index_name)
# #     print("Index exists")
# #     idx = True
# # except elasticsearch.NotFoundError:
# #     print("Index does not exist")
# #     idx = False

# print(f"Does Idx exists: {idx} : {type(idx)}")
if not idx:
    print(f"Index '{index_name}', does not exists so creating it.")
    es.indices.create(index=index_name)
# es.indices.create(index=index_name)

# Parse and send logs
with open(fileName, "r") as file:
    for line in file:
        try:
            # Parse line format: "2025-06-20 12:34:56 [INFO] Message..."
            timestamp_str, rest = line.split(" [", 1)
            level_, message_ = rest.split("] ", 1)
            # print(f"Ts: {timestamp_str}, Rest: {rest}")
            # print(f"Level: {level_}, Message: {message_}")
            ts = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            lvl = level_.strip()
            message = message_.strip()
            # print(message)
            doc = {
                "timestamp": ts,
                "level": lvl,
                "message": message_
            }

            if "ERROR" == lvl:
                error_type = re.search(r"(\w+): ", message)
                if error_type:
                    doc["error_type"] = error_type.group(1)
                    # print(f"Error: {error_type.groups(1)}")

            if "Execution time" in message:
                # print("ExecTime IN : ",re.search(r"Execution time: ([\d.]+)s", message))
                exec_match = re.search(r"Execution time: ([\d.]+)s", message)
                if exec_match:
                    doc["execution_time"] = float(exec_match.group(1))

            if "Affected rows" in message:
                # print("AffRows IN")
                table_match = re.search(r"table (\w+)", message)
                rows_match = re.search(r"Affected rows: (\d+)", message)
                query_type = re.search(r"(\w+) query", message)
                if table_match:
                    doc["table_name"] = table_match.group(1)
                if rows_match:
                    doc["affected_rows"] = int(rows_match.group(1))
                if query_type:
                    doc["query_type"] = query_type.group(1)
                # print(f"QueryMatch: '{query_type.group(1)}'")

            if "Rows returned" in message:
                table_match = re.search(r"table (\w+)", message)
                rows_match = re.search(r"Rows returned: (\d+)", message)
                if table_match:
                    doc["table_name"] = table_match.group(1)
                if rows_match:
                    doc["rows_returned"] = int(rows_match.group(1))
                doc["query_type"] = "SELECT" # rows_returned are from SELECT queries only in this example

            es.index(index=index_name, document=doc)
            print(f"Doc:: {doc}")
        except Exception as e:
            print(f"Failed to process line: {line.strip()} - {e}")
