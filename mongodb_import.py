import csv
import ast  # Library to parse strings as Python literals
from pprint import pprint
# Read the CSV file and format the 'artists' and 'artist_ids' columns

rel = []

with open('sample.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    data = list(reader)

    # Loop through the rows and format the 'artists' and 'artist_ids' columns
    for row in data:
        # Convert 'artists' column from string to list
        row['artists'] = ast.literal_eval(row['artists'])

        # Convert 'artist_ids' column from string to list
        row['artist_ids'] = ast.literal_eval(row['artist_ids'])
            

albums = [{'name': item['album'], 'id': item['album_id']} for item in data]
artists = [{'name': name, 'id': artist_id} for item in data for name, artist_id in zip(item['artists'], item['artist_ids'])]


# for item in data:
#     del item['album']
#     del item['album_id']
#     del item['artist_ids']
#     del item['artists']

pprint(rel)

# Importing into MongoDB
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Update with your MongoDB connection URI
db = client['TracksDB']

# Inserting Tracks Data
collection = db['Tracks']

# Insert many documents into the collection
result = collection.insert_many(data)


# Inserting Albums 
# Print the inserted document IDs
print("MongoDB Import. Done")

# Importing into Neo4J

