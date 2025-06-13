from flask import Flask, request, render_template
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch

app = Flask(__name__)
model = SentenceTransformer("all-MiniLM-L6-v2")

url="https://localhost:9200"
uname="elastic"
upwd="SYq1WX13c91kc88Q3Ulzo9X2"
es = Elasticsearch(hosts=[url], verify_certs=False, basic_auth=(uname,upwd), request_timeout=60 )
INDEX_NAME = "countriesdata_with_vectors"

FIELD_MAX = {
    "Population": 1_400_000_000,
    "Area": 17_000_000,
    "Birthrate": 50.0
}

BOOST_WEIGHT = 0.3

def detect_boost_field(query):
    q = query.lower()
    if "largest population" in q or "most populous" in q or "highest population" in q:
        return "Population"
    elif "largest area" in q or "biggest country" in q:
        return "Area"
    elif "highest birthrate" in q:
        return "Birthrate"
    return None

@app.route("/", methods=["GET", "POST"])
def index():
    results = []
    query = ""
    boost_field = None

    if request.method == "POST":
        query = request.form["query"]
        boost_field = detect_boost_field(query)
        query_vector = model.encode(query).tolist()

        if boost_field:
            max_val = FIELD_MAX[boost_field]
            script_query = {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": (
                            "cosineSimilarity(params.query_vector, 'embedding_vector') + "
                            f"{BOOST_WEIGHT} * doc['{boost_field}'].value / {max_val}"
                        ),
                        "params": {"query_vector": query_vector}
                    }
                }
            }
        else:
            script_query = {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'embedding_vector')",
                        "params": {"query_vector": query_vector}
                    }
                }
            }

        es_res = es.search(index=INDEX_NAME, body={
            "size": 10,
            "query": script_query,
            "_source": ["Country", "Region", "Population", "Area", "Birthrate"]
        })

        results = [
            {
                "country": hit["_source"]["Country"],
                "region": hit["_source"]["Region"],
                "population": hit["_source"]["Population"],
                "area": hit["_source"]["Area"],
                "birthrate": hit["_source"].get("Birthrate",0.0),
                "score": round(hit["_score"], 4)
            }
            for hit in es_res["hits"]["hits"]
        ]

    return render_template("index.html", results=results, query=query, boost=boost_field)

if __name__ == "__main__":
    app.run(debug=True)
