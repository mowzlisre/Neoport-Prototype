import csv
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

print(">>> Preprocessing - Phase II")
                
albums_data = [dict(t) for t in {tuple(d.items()) for d in albums_data}]

print(">>> Attempting to establish connection with MongoDB Client")


# Importing into MongoDB
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Update with your MongoDB connection URI

db = client['TracksDB']

print(f">>> Attempting to Insert {len(data)} documents in Tracks collection")
# Inserting Tracks Data
tracks = db['Tracks']

# Insert many documents into the collection
result = tracks.insert_many(data)
print(">>> Inserting documents in Tracks collection successful")

print(f">>> Attempting to Insert {len(albums_data)} documents in Albums collection")
# Inserting Albums 
albums = db["Albums"]

# Insert many documents into the collection
result = albums.insert_many(albums_data)
print(">>> Inserting documents in Albums collection successful")

print(f">>> Attempting to Insert {len(artists_data)} documents in Artists collection")
# Inserting Artists 
artists = db["Artists"]

# Insert many documents into the collection
result = artists.insert_many(artists_data)
print(">>> Inserting documents in Artists collection successful")

# Print the inserted document IDs
print(">>> Closing MongoDB connection!")
client.close()
# Importing into Neo4J

