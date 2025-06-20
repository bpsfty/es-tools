"""
This program is intended to convert used query in natural language to a Kibana query.
"""
import os
from openai import AzureOpenAI
from elasticsearch import Elasticsearch
import dotenv
from common_variables import *

def process_user_prompt_for_ai(prompt):
    return f"""
    You are an expert in Elasticsearch. Convert the following natural language request into Elasticsearch Query DSL (JSON format) that can be run directly:
    Query: "{prompt}"
    Just return the JSON DSL only.
    """
def process_ai_response(ai_response):
    print(f"*** Raw response: ***\n{ai_response}")
    # Optional: Clean up unwanted preamble or code block markers
    #output = output.strip().strip("```json").strip("```")
    outputLst = ai_response.split("```")
    #outputLst = output.split("/_search")
    print(outputLst)
    #output = outputLst[1].strip("```")
    return outputLst[1].strip("json")

dotenv.load_dotenv()
client = AzureOpenAI(api_key=oa_api_key, api_version=oa_api_version, azure_endpoint=oa_endpoint)
es = Elasticsearch(hosts=[es_url], verify_certs=False, basic_auth=(uname, upwd))  # Elasticsearch(es_url)

print("Please enter your query for elasticsearch index in natural language at the Prompt. ('q' to quit)")
while True:
    prompt = input("Prompt")
    if prompt in ('q','Q'): break
    prompt_u = process_user_prompt_for_ai(prompt)
    print("\nGenerating Elasticsearch DSL using AzureOpenAI...")
    completion = client.chat.completions.create(model=oa_deployment, messages=[{'role': 'user', 'content': prompt_u}])
    ai_response = completion.choices[0].message.content
    #print(f"{completion.choices[0].message.role}: {ai_response}")
    es_dsl_query = process_ai_response(ai_response)

    print("\nQuerying Elasticsearch...")
    es_response = es.search(index=index_name, body=es_dsl_query)

    print("\nElasticsearch result:")
    for hit in es_response['hits']['hits']:
        print(hit['_source'])

