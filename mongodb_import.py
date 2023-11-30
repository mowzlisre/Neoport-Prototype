import csv
import ast  # Library to parse strings as Python literals
from pprint import pprint
# Read the CSV file and format the 'artists' and 'artist_ids' columns

print(">>> Attempting to import the CSV")
with open('sample.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    data = list(reader)
    print(">>> CSV Imported")
    # Loop through the rows and format the 'artists' and 'artist_ids' columns
    print(">>> Starting to Preprocess the data to evaluate the data types")
    print(">>> Preprocessing - Phase I")
    for row in data:
        # Convert 'artists' column from string to list
        row['artists'] = ast.literal_eval(row['artists'])

        # Convert 'artist_ids' column from string to list
        row['artist_ids'] = ast.literal_eval(row['artist_ids'])
            

print(">>> Preprocessing - Phase II")
albums = [{'name': item['album'], 'id': item['album_id']} for item in data]
artists = [{'name': name, 'id': artist_id} for item in data for name, artist_id in zip(item['artists'], item['artist_ids'])]

print(">>> Preprocessing - Phase III")
data = [{k: v for k, v in item.items() if k != 'album'} for item in data]
data = [{k: v for k, v in item.items() if k != 'artists'} for item in data]

print(">>> Preprocessing completed!")
# for item in data:
#     del item['album']
#     del item['album_id']
#     del item['artist_ids']
#     del item['artists']

print(">>> Attempting to conenct to MongoDB Client")
# Importing into MongoDB
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Update with your MongoDB connection URI

db = client['TracksDB']

print(">>> Attempting to Insert documents in Tracks collection")
# Inserting Tracks Data
tracks = db['Tracks']

# Insert many documents into the collection
result = tracks.insert_many(data)
print(">>> Inserting documents in Tracks collection successful")

print(">>> Attempting to Insert documents in Albums collection")
# Inserting Albums 
albums = db["Albums"]

# Insert many documents into the collection
result = albums.insert_many(albums)
print(">>> Inserting documents in Albums collection successful")

print(">>> Attempting to Insert documents in Artists collection")
# Inserting Artists 
artists = db["Artists"]

# Insert many documents into the collection
result = artists.insert_many(artists)
print(">>> Inserting documents in Artists collection successful")


# Print the inserted document IDs
print(">>> Ending MongoDB Data Insertion!")

# Importing into Neo4J

