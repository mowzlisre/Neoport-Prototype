#%%
from neo4j import GraphDatabase
import csv
import ast  # Library to parse strings as Python literals
from pprint import pprint
# Read the CSV file and format the 'artists' and 'artist_ids' columns

rel = []

with open('tracks_features.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    data = list(reader)
    print("CSV Import done")
    # Loop through the rows and format the 'artists' and 'artist_ids' columns
    for row in data:
        # Convert 'artists' column from string to list
        row['artists'] = ast.literal_eval(row['artists'])

        # Convert 'artist_ids' column from string to list
        row['artist_ids'] = ast.literal_eval(row['artist_ids'])
        # for item in row['artist_ids']:
        #     obj = {
        #         "source" : item,
        #         "target" : row["id"],
        #         "type" : "CONTRIBUTED_TO"
        #     }
        #     rel.append(obj)
            

albums = [{'name': item['album'], 'id': item['album_id']} for item in data]
artists = [{'name': name, 'id': artist_id} for item in data for name, artist_id in zip(item['artists'], item['artist_ids'])]


# Your nodes as a list of dictionaries

uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"

print("Preprocessing done")
#%%
# Connect to Neo4j
driver = GraphDatabase.driver(uri, auth=(username, password))


with driver.session() as session:
    print("Connection established!")
    # Run Cypher query to bulk insert nodes
    session.run("""
        UNWIND $nodes AS node
        MERGE (_album:Album {
            album_name: node.album,
            album_id: node.album_id
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
    """, nodes=data[:100000])
    

driver.close()

# %%
print(data)
# %%
