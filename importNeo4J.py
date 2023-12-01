#%%
from neo4j import GraphDatabase
import csv, re
import ast  # Library to parse strings as Python literals
from pprint import pprint
# Read the CSV file and format the 'artists' and 'artist_ids' columns



with open('tracks_features.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    data = list(reader)
    print(">>> CSV Imported")
    print(">>> Starting to Preprocess the data to evaluate the data types")
    print(">>> Preprocessing - Phase I")

    albums_data = []
    artists_data = []
    existing_artist_ids = set()  # Use a set for faster lookup

    for row in data:
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

# Your nodes as a list of dictionaries

uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"

print("Preprocessing done")
# Connect to Neo4j
#%%
def generate_number_sequence(n):
    result = []
    div = 10000
    while div <= n:
        result.append(div)
        div += 10000
    if result[-1] != n:
        result.append(n)
    return result

if len(data) > 10000:
    batches = generate_number_sequence(len(data))
else:
    batches = [10000]

prev = 0
for index, batch in enumerate(batches):
    driver = GraphDatabase.driver(uri, auth=(username, password))

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
    print(f"Batch {index+1} insertion done")
    prev = batch

    driver.close()
# %%
