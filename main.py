#%%
from neo4j import GraphDatabase
import csv, re
import ast, time
import multiprocessing
from support import *

albums_data = []
artists_data = []
existing_artist_ids = set()  # Use a set for faster lookup

def preprocess(rows):
    for row in rows:
        row['artists'] = ast.literal_eval(row['artists'])
        row['artist_ids'] = ast.literal_eval(row['artist_ids'])
        
        albums_data.append({
            "name": row['album'],
            "_id": row["album_id"]
        })

        for index, artist in enumerate(row['artist_ids']):
            if artist not in existing_artist_ids:
                artists_data.append({
                    "name": row['artists'][index],
                    "_id": artist
                })
                existing_artist_ids.add(artist) 
        integer = r'^-?\d+$'
        flt = r'^-?\d+(\.\d+)?$'
        for key, value in row.items():
            if isinstance(value, str):
                if re.match(integer, value):
                    if int(value) < 2147483647:
                        row[key] = int(value)
                elif re.match(flt, value):
                    row[key] = float(value)
        
        row["explicit"] = True if row["explicit"] == "True" else False

if __name__ == "__main__":
    x = time.time()
    with open('tracks_features.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        data = list(reader)
        print(">>> CSV Imported")
        print(">>> Starting to Preprocess the data to evaluate the data types")
        print(">>> Preprocessing - Phase I")

    # Split data into chunks for parallel processing
    num_chunks = multiprocessing.cpu_count()  # Number of chunks equal to CPU cores
    chunk_size = len(data) // num_chunks
    data_chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

    # Create a Pool of processes
    with multiprocessing.Pool() as pool:
        pool.map(preprocess, data_chunks)
    y = time.time()
    print(f"Preprocess ended in {y-x}s")

    if len(data) > 50000:
        batches = generate_number_sequence(len(data))
    else:
        batches = [50000]

    prev = 0

    for index, batch in enumerate(batches):
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
        with driver.session() as session:
            print(f"Inserting Batch {index+1} of {len(batches)}")
            # Run Cypher query to bulk insert nodes
            session.run("""
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
                    MERGE (_artist:Artist {
                        id: node.artist_ids[idx],
                        artist_name: node.artists[idx]
                    })
                    MERGE (_artist)-[:Contributed]->(_album)
                )
            """, nodes=data[prev:batch])
        prev = batch

        driver.close()
    
    x = time.time()
    print(f"Inserting ended in {x-y:.2f}s")
# %%
