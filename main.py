# Step 1
# Reading the CSV file
import csv, ast

# Initialize an empty list to store the data
data = []

# Path to your CSV file

# Reading the CSV file and converting it into Python data
start = 192121  # Specify the starting row number (inclusive)
end = 200000   # Specify the ending row number (exclusive)

with open('./tracks_features.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    current_row = 0
    
    # Move to the start position
    while current_row < start:
        next(reader)  # Skip rows until the start position
        current_row += 1
    
    # Read rows within the specified range
    for row in reader:
        # Check if the end position is reached
        if current_row >= end:
            break
        
        data.append(row)
        current_row += 1


# Step 2
# Clean the Dataset

# Step 3

# Step Inserting

from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"

driver = GraphDatabase.driver(uri, auth=(username, password))

def insert_data(tx, item):
    item["artists"] = ast.literal_eval(item["artists"])
    item["artist_ids"] = ast.literal_eval(item["artist_ids"])
    item["artist_names"] = ", ".join(item["artists"])
    item["artist_ids"] = item["artist_ids"] if isinstance(item["artist_ids"], list) else [item["artist_ids"]]

    query = """
        CREATE (track:Track {
            id: $id,
            name: $name,
            track_number: $track_number,
            disc_number: $disc_number,
            explicit: $explicit,
            danceability: $danceability,
            energy: $energy,
            key: $key,
            loudness: $loudness,
            mode: $mode,
            speechiness: $speechiness,
            acousticness: $acousticness,
            instrumentalness: $instrumentalness,
            liveness: $liveness,
            valence: $valence,
            tempo: $tempo,
            duration_ms: $duration_ms,
            time_signature: $time_signature,
            year: $year,
            release_date: $release_date,
            artist_names: $artist_names
        })
        MERGE (_album:Album {
            album_name: $album,
            album_id: $album_id
        })
        WITH track, _album
        MERGE (_album)-[:ContainsTrack]->(track)
    """
    for index, artist_id in enumerate(item["artist_ids"]):
        artist_id_value = item["artist_ids"][index].replace("'", "\\'") if "'" in item["artist_ids"][index] else item["artist_ids"][index].replace('"', '\\"')
        artist_name_value = item["artist_names"].split(',')[index].replace("'", "\\'") if "'" in item["artist_names"].split(',')[index] else item["artist_names"].split(',')[index].replace('"', '\\"')
    
        query += f"""
            MERGE (_artist{index}:Artist {{id: '{artist_id_value}', artist_name: "{artist_name_value}"}})
            WITH _artist{index}, _album
            MERGE (_artist{index})-[:Contributed]->(_album)
        """


    tx.run(query, **item)

# Your 'data' variable containing the list of track information
ind = 0
with driver.session() as session:
    for item in data:
        session.write_transaction(insert_data, item)
        print(f"Row {start+ind} is inserted of {end}")
        ind += 1

