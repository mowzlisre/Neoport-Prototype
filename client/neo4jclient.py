from neo4j import GraphDatabase


uri = "neo4j://localhost:7687"  
username = "neo4j" 
password = "12345678" 

def client(cypher_query):
    # Establish connection to the Neo4j database
    driver = GraphDatabase.driver(uri, auth=(username, password))


    # Execute the Cypher query within a session
    with driver.session() as session:
        result = session.run(cypher_query)

        records_list = []

        for record in result:
            record_object = {}
            for key in record.keys():
                record_object[key] = record[key]
            records_list.append(record_object)

        
    # Close the Neo4j driver when done
    driver.close()
    return records_list