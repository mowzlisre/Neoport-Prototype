from client.query import queries
import time
from client import mongoclient, neo4jclient

def proccess_exec():
    for index, query in enumerate(queries):
        print(f">>> Query {index+1}: {query['description']}")
        mongo_init = time.time()
        mongo = mongoclient.client(query["mongo"], 'Tracks', 'aggr')
        mongo_end = time.time()
        mongo_diff = mongo_end - mongo_init
        print(f">>> Time taken by MongoDB to finish the query: {mongo_diff:.3f}s")
        neo_init = time.time()
        neo = neo4jclient.client(query["neo"])
        neo_end = time.time()
        neo_diff = neo_end - neo_init
        print(f">>> Time taken by Neo4J to finish the query: {neo_diff:.3f}s")

        if mongo_diff < neo_diff:
            print(f">>> MongoDB executed {(neo_diff-mongo_diff) * 100 / neo_diff:.2f}% faster than Neo4J")
        else:
            print(f">>> Neo4J executed {(mongo_diff-neo_diff) * 100 / mongo_diff:.2f}% faster than MongoDB")
        
        print('\n')
    return [mongo_diff, neo_diff]

if __name__ == '__main__':
    mongo = []
    neo = []
    for i in range(0,10):
        a, b = proccess_exec()
        mongo.append(a)
        neo.append(b)
    print(mongo, neo)