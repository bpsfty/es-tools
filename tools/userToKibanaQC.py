"""
This program is intended to convert used query in natural language to a Kibana query and use to query elasticsearch.
"""
import requests
from elasticsearch import Elasticsearch
import json
from common_variables import *
#from model_context import get_context  # MCP integration

index_name="countriesdata"
#model_name = "llama3.1:8b"
#mcp_context_id = "mycontext"


# --- FUNCTION: Generate Query DSL using Ollama ---
def generate_query_dsl(natural_language_query):
    prompt = f"""
You are an expert in Elasticsearch. Convert the following natural language request into Elasticsearch Query DSL (JSON format) that can be run directly:
Query: "{natural_language_query}"
Just return the JSON DSL only.
"""
    response = requests.post(ollama_url, json={"model": model_name, "prompt": prompt, "stream": False})
    response.raise_for_status()
    output = response.json()["response"]
    print(f"*** Raw response: {output}")
    # Optional: Clean up unwanted preamble or code block markers
    #output = output.strip().strip("```json").strip("```")
    outputLst = output.split("```")
    #outputLst = output.split("/_search")
    print(outputLst)
    #output = outputLst[1].strip("```")
    return outputLst[1].strip("json")

# --- FUNCTION: Query Elasticsearch using MCP ---
def query_elasticsearch_with_mcp(dsl_query):
    es = Elasticsearch(hosts=[es_url], verify_certs=False, basic_auth=(uname,upwd) ) #Elasticsearch(es_url)
    context = get_context(mcp_context_id)
    query_dict = eval(dsl_query) if isinstance(dsl_query, str) else dsl_query
    response = context.client.search(index=index_name, body=query_dict)
    return response

# --- STEP 2: Query MCP Server using the DSL ---
def run_dsl_through_mcp(index: str, dsl: str) -> dict:
    headers = {"Content-Type": "application/json"}
    payload = {
        "context": {
            "provider": "elasticsearch",
            "config": {
                "es_host": es_url,
                "verify_certs": False
            }
        },
        "operation": {
            "operation": "search",
            "params": {
                "index": index,
                "body": json.loads(dsl)
            }
        }
    }

    response = requests.post(MCP_ENDPOINT, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def query_elasticsearch_without_mcp(dsl_query):
    es = Elasticsearch(hosts=[es_url], verify_certs=False, basic_auth=(uname,upwd) ) #Elasticsearch(es_url)
    response = es.search(index=index_name, body=dsl_query)
    return response

if __name__ == "__main__":
    user_query = input("Ask your question: ")

    print("\n💬 Generating Elasticsearch DSL using Ollama...")
    dsl_query = generate_query_dsl(user_query)
    print("\n📄 Generated DSL:\n", dsl_query)

    print("\n🔍 Querying Elasticsearch without MCP...")
    results = query_elasticsearch_without_mcp(dsl_query) #query_elasticsearch_with_mcp(dsl_query)

    print("\n📊 Search Results:")
    for hit in results['hits']['hits']:
        print(hit['_source'])


