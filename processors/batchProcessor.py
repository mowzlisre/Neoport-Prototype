from support import *
from neo4j import GraphDatabase

def batch_process(_data, query, rel):
    if rel:
        if len(_data) > 100:
            batches = generate_number_sequence(len(_data), 100)
        else:
            batches = [100]
    else:
        if len(_data) > CHUNK:
            batches = generate_number_sequence(len(_data), CHUNK)
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
            # print(f"{prev} - {batch} batch")
            result = session.run(query, nodes=_data[prev:batch])
            print(f">>> Batch {index+1} of {len(batches)} imported")
            prev = batch

    driver.close()
    print(">>> Safely closing the connection with Neo4J Database Server")

def indexing():
    driver = GraphDatabase.driver(NEO_URI, auth=(USERNAME, PASSWORD))
    with driver.session() as session:
        session.run("CREATE INDEX FOR (a:Album) ON (a.id)")
        session.run("CREATE INDEX FOR (t:Track) ON (t.id)")
        session.run("CREATE INDEX FOR (b:Artist) ON (b.id)")
        print(f">>> Nodes Indexed")

    driver.close()