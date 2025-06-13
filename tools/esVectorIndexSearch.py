from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch

url="https://localhost:9200"
uname="elastic"
upwd="SYq1WX13c91kc88Q3Ulzo9X2"

#vector_index_name = "countriesdata_with_vectors"
vector_index_name = "countriesdata_with_vectors_mpnet"
es = Elasticsearch(hosts=[url], verify_certs=False, basic_auth=(uname,upwd), request_timeout=60 )

#model = SentenceTransformer("all-MiniLM-L6-v2")  # 384-dim vectors
model = SentenceTransformer("all-mpnet-base-v2")  # 768-dim vectors

#query = "which country has the largest area"
while True:
    query = input("\nEnter your query (press 'q' to quit): ")
    if query == 'q': break

    sortField = None
    sortOrder = "asc"
    if "population" in query or "populous" in query: sortField = "Population"
    elif "area" in query or "largest" in query: sortField = "Area"
    elif "birthrate" in query: sortField = "Birthrate"

    if "largest" in query or "most" in query or "highest" in query: sortOrder = "desc"
    #elif "smallest" in query or "least" in query or "lowest" in query: sortOrder = "Asc"

    query_vector = model.encode(query).tolist()
    print(query_vector)
    searchBody = {
        #"size": 6,
        "knn": {
            "field": "embedding_vector",
            "k": 5,
            "num_candidates": 200,
            "query_vector": query_vector
        },
        "_source": ["Country","Region","Population", "Birthrate", "Area"]
        #,"sort": [{"Population": {"order": "desc"}}]
    }
    if sortField:
        searchBody["sort"] = [{sortField : {"order": sortOrder}}]
        searchBody["sort"] = [{"_score": {"order": sortOrder}}]


    results = es.search(index=vector_index_name, body=searchBody)

    print("Score | Country | Population | Area (sq.mi.) | BirthRate | Region")
    for hit in results["hits"]["hits"]:
        #print(f"{hit['_source']['Country']} : {hit['_source']['Population']} — Score: {hit['_score']}")
        score = hit.get("_score")
        src = hit["_source"]
        #countwhich country has the largest populationry = src["Country"]
        #text = ""#hit["_source"]["Content"]
        print(f"{score if score is not None else 0.0:.3f} | {src['Country']} | {src['Population']:,} | {src['Area']:,} | {src["Birthrate"]} | {src['Region']} ")