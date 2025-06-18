'''
*** Program to read a csv file, convert it to json, and populate it to elasticsearch index ***
* ensure csv is having header row (else it will take first row as heading)
* this assumes elasticsearch is running at localhost:9200, with https.
* elastic user and password needs to be modified.
* SSL certificate verification is disabled.
* this program creates docs in index starting with index 0. (So if data is already present it would be overwritten.)
'''
import pandas as pd
import json
import requests
from common_variables import *

indexName="countriesdata"
fileName="./countries of the world.csv"
headers = {"Content-Type": "application/json"}
user=""
pwd=""
#url="https://localhost:9200"
##if index also needs to be created
idxCreationParams = "{ \"settings\": {\"number_of_shards\": 2, \"number_of_replicas\": 1} }"
response = requests.put(es_url+"/"+indexName,auth=(user,pwd),verify=False, headers=headers, data=idxCreationParams)
print(response.text)

# Load CSV file
##"C:/bhupinder/samples/elasticsearch-sample-index/indexme.csv"
##"C:/bhupinder/elk/countries of the world.csv"
df = pd.read_csv(fileName)  # Replace with your file path
#print(df.head())  # Preview the data
print("Size : ",df.size)
#print(df.iloc[0:1,:])
#print("\n####")
'''for i in range (3):
    print(df.iloc[i])'''

with open("output.jsonl", "w") as f:
    x=0
    for _, row in df.iterrows():
        f.write("{ \"index\" : {\"_index\":\""+indexName+"\", \"_id\": \"" + str(x) + "\"}}\n")  # Custom separator
        x=x+1
        json.dump(row.to_dict(), f)
        f.write("\n")  # Ensure each JSON object is on a new line


with open("output.jsonl", "r") as f:
    json_data = f.read()

#print(json_data)
response = requests.put(url+"/_bulk",auth=(user,pwd),verify=False, headers=headers, data=json_data)
print(response.text)
