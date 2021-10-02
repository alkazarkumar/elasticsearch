import logging
from elasticsearch import Elasticsearch
import json
# this is the file.

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host':'localhost', 'port': 9200}])
    if _es.ping():
        print("Connected")
    else:
        print("Not Connected")
    
    # returning elastic search object
    return _es


def create_index(es_object,index_name):
    created = False

    # This is Mapping which is the Elastic’s terminology for a schema.
    #    http://localhost:9200/recipes/_mappings gives this settings
    # 
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "members": {
                "dynamic": "strict",
                "properties": {
                    "name": {
                        "type": "text"
                    },
                    "position": {
                        "type": "text"
                    },
                    "salary": {
                        "type": "integer"
                    },
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print("Index not created")
        print(str(ex))
    finally:
        return created


def create_document(es_object, index_name, record):
    try:
        outcome = es_object.index(index=index_name, doc_type='second_type', body=record)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))

def search_document(es_object,index_name,salary):
    search_object = {'query': {'match': {'salary': salary}}}
    result = es_object.search(index=index_name,body=json.dumps(search_object))
    print(result)

    

def search_extensive(es_object,index_name,salary):
    search_object = {'_source': ['title'], 'query': {'range': {'salary': {'gte': salary}}}}
    result = es_object.search(index=index_name,body=json.dumps(search_object))
    print(result)

    
    
 curl -POST http://localhost:9200/my_index/my_type -curl -H 'Content-Type: application/json' -d '{"user":"Phil","message":"Hello World!"}'
 # request
curl -GET http://localhost:9200/my_index/my_type/G123

# response
{"_index":"my_index","_type":"my_type","_id":"G123","_version":2,"found":true,"_source":{"user":"Phil","message":"Hello, World!" }}
# request
curl -PUT http://localhost:9200/my_index/my_type/G123?version=1 -curl -H 'Content-Type: application/json' -d '{"user":"Phil","message":"Hello, World!"}'

# response
{"_index":"my_index","_type":"my_type","_id":"G123","_version":2,"result":"updated","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}
    
   

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    es_object = connect_elasticsearch()
    # run it for 1 time ONLY
    #   if es_object :
    #       create_index(es_object,"second_index")

    # For Insertion
    index_name = "second_index"
    # if es_object :
    #     for i in range(10):
    #         record = {
    #             "name" : "new"+str(i),
    #             "position" : "intern"+str(i),
    #             "salary" : int(10000)+int(i)
    #         }
    #         create_document(es_object,index_name,record)
    # if es_object:
    #     search_document(es_object,index_name,10001)
    if es_object:
        search_extensive(es_object,index_name,10006)
