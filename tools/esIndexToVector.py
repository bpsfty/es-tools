"""
This program takes an already existing index and creates a new vector index based on it.
"""
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch

i=0
url="https://localhost:9200"
uname="elastic"
upwd="SYq1WX13c91kc88Q3Ulzo9X2"

source_index = "countriesdata"
vector_index_name = "countriesdata_with_vectors_mpnet"

es = Elasticsearch(hosts=[url], verify_certs=False, basic_auth=(uname,upwd), request_timeout=60 )

mapping = {
    "mappings": {
        "properties": {
            "Content": {"type": "text"},
            "embedding_vector": {
                "type": "dense_vector",
                "dims": 768,
                "index": True,
                "similarity": "cosine"
            },
            "Country": {"type": "text"}, #keyword
            "Region": {"type": "text"}, #keyword
            "Population": {"type": "integer"},
            "Area": {"type": "float"},
            "Birthrate": {"type": "float"},
            "Deathrate": {"type": "float"},
            "Literacy": {"type": "float"},
            "CoastlineRatio": {"type": "float"}
        }
    }
}

# Create the new index
es.indices.create(index=vector_index_name, body=mapping, ignore=400)

#model = SentenceTransformer("all-MiniLM-L6-v2")  # 384-dim vectors
model = SentenceTransformer("all-mpnet-base-v2")  # 768-dim vectors

# Fetch data from your existing index
scroll = es.search(index=source_index, scroll="1m", size=100)

scroll_id = scroll["_scroll_id"]
hits = scroll["hits"]["hits"]

def convertToFloat(val):
    if val:
        try:
            return float(val)
        except ValueError:
            return None
    else:
        return None

while hits:
    for hit in hits:
        doc = hit["_source"]
        # Choose fields to encode — tweak this logic as needed
        #text = f"{doc.get('Country', '')}, {doc.get('Region', '')}. Population: {doc.get('Population', '')}"
        area = int(doc.get('Area (sq. mi.)'))
        coastlineRatio = convertToFloat(doc.get('Coastline (coast/area ratio)').replace(",",".")) #float(doc.get('Coastline (coast/area ratio)').replace(",","."))
        birthRate = convertToFloat(doc.get('Birthrate').replace(",",".")) #float(doc.get('Birthrate',0).replace(",","."))
        deathRate = convertToFloat(doc.get('Deathrate').replace(",",".")) #float(doc.get('Deathrate',0).replace(",","."))
        literacyRate = convertToFloat(doc.get('Literacy (%)').replace(",",".")) #float(doc.get('Literacy (%)').replace(",","."))
        text = f"{doc.get('Country', '')} is a country in {doc.get('Region', '')}, having a population of {doc.get('Population', '')}. It covers an area of {area} sq. miles. with a coastline ratio of {coastlineRatio}. Its birthrate is {birthRate} and deathrate is {deathRate}. Its literacy rate is {literacyRate}"
        #text = f"{doc.get('Country', '')} is a country in {doc.get('Region', '')}, having a population of {doc.get('Population', '')}. It covers an area of {doc.get('Area (sq. mi.)', '')} sq. miles. with a coastline ratio of {doc.get('Coastline (coast/area ratio)', '')}. Its birthrate is {doc.get('Birthrate', '')} and deathrate is {doc.get('Deathrate', '')}. Its literacy rate is {doc.get('Literacy (%)','')}"
        embedding = model.encode(text).tolist()

        newDoc = {}
        newDoc.update({"Country": doc.get('Country')})
        newDoc.update({"Region": doc.get('Region')})
        newDoc.update({"Population": doc.get('Population')})
        newDoc.update({"Area": area})
        newDoc.update({"CoastlineRatio": coastlineRatio})
        newDoc.update({"Birthrate": birthRate})
        newDoc.update({"Deathrate": deathRate})
        newDoc.update({"Literacy": literacyRate})
        newDoc.update({"Content": text})
        #print(type(newDoc))
        print(newDoc)
        # Add embedding and index into new index
        newDoc.update({"embedding_vector": embedding})
        es.index(index=vector_index_name, body=newDoc)
        i+=1

    # Scroll for next batch
    scroll = es.scroll(scroll_id=scroll_id, scroll="1m")
    hits = scroll["hits"]["hits"]

print(f"i: {i}")