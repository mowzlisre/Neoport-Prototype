from support import *
from neo4j import GraphDatabase

def importDB(data):
    # Creating chunks of data for batch processing
    if len(data) > CHUNK:
        batches = generate_number_sequence(len(data))
    else:
        batches = [CHUNK]


    print(f">>> Attempting to import data in {len(batches)} batches")
    print(f">>> Establishing connection with Neo4J Database Server at {NEO_URI}")

    # Connect to Neo4J
    
    prev = 0
    for index, batch in enumerate(batches):
        driver = GraphDatabase.driver(NEO_URI, auth=(USERNAME, PASSWORD))

        with driver.session() as session:
            # Run Cypher query to bulk insert nodes
            result = session.run("""
                UNWIND $nodes AS node
                MERGE (_album:Album {
                    name: node.album,
                    id: node.album_id
                }) 
                CREATE (track:Track {
                    id: node.id,
                    name: node.name,
                    track_number: node.track_number,
                    disc_number: node.disc_number,
                    explicit: node.explicit,
                    danceability: node.danceability,
                    energy: node.energy,
                    key: node.key,
                    loudness: node.loudness,
                    mode: node.mode,
                    speechiness: node.speechiness,
                    acousticness: node.acousticness,
                    instrumentalness: node.instrumentalness,
                    liveness: node.liveness,
                    valence: node.valence,
                    tempo: node.tempo,
                    duration_ms: node.duration_ms,
                    time_signature: node.time_signature,
                    year: node.year,
                    release_date: node.release_date
                })  
                MERGE (_album)-[:ContainsTrack]->(track)
                FOREACH(idx IN RANGE(0, size(node.artists) - 1) |
                    CREATE (_artist:Artist {
                        id: node.artist_ids[idx],
                        artist_name: node.artists[idx]
                    })
                    MERGE (_artist)-[:Contributed]->(_album)
                )
            """, nodes=data[prev:batch])
            print(result)
        print(f">>> Batch {index+1} of {len(batches)} imported")
        prev = batch

        driver.close()
    print(">>> Safely closing the connection with Neo4J Database Server")
    return len(batches)
