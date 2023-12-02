from support import *
from neo4j import GraphDatabase

def batch_process(_data, query, rel):
    if rel:
        batches = [' '.join(_data[i:i + CHUNK]) for i in range(0, len(_data),CHUNK)]
        
        print(f">>> Attempting to import data in {len(batches)} batches")
        print(f">>> Establishing connection with Neo4J Database Server at {NEO_URI}")
        
        driver = GraphDatabase.driver(NEO_URI, auth=(USERNAME, PASSWORD))
        with driver.session() as session:
            for index, batch in enumerate(batches):
                print(batch)
                session.run(batch)  
                print(f">>> Batch {index+1} of {len(batches)} imported")
                prev = batch

        driver.close()
    else:
        if len(_data) > CHUNK:
            batches = generate_number_sequence(len(_data))
        else:
            batches = [CHUNK]

        print(f">>> Attempting to import data in {len(batches)} batches")
        print(f">>> Establishing connection with Neo4J Database Server at {NEO_URI}")

        # Connect to Neo4J

        prev = 0
        driver = GraphDatabase.driver(NEO_URI, auth=(USERNAME, PASSWORD))
        with driver.session() as session:
            for index, batch in enumerate(batches):
                # Run Cypher query to bulk insert nodes
                session.run(query, nodes=_data[prev:batch])
                print(f">>> Batch {index+1} of {len(batches)} imported")
                prev = batch

        driver.close()
        print(">>> Safely closing the connection with Neo4J Database Server")
