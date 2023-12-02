from pymongo import MongoClient
from support import *

def importDB(data, albums_data, artists_data):

    print(f">>> Establishing connection with MongoDB Server at {MGDB_URI}")
    
    # Connect to MongoDB
    client = MongoClient(MGDB_URI)  # Update with your MongoDB connection URI
    
    # Creating new Database
    print(">>> Creating Database `{MGDB_DBN}`")
    db = client[MGDB_DBN]

    # Creating new Collection `Tracks`
    print(">>> Creating Collection `Tracks`")
    print(f">>> Attempting to Insert {len(data)} documents in Tracks collection")
    
    # Inserting Tracks Data
    tracks = db['Tracks']

    # Insert many documents into the collection
    result = tracks.insert_many(data)
    print(result)

    print(">>> Inserting documents in Tracks collection successful")

    # Creating new Collection `Albums`
    print(">>> Creating Collection `Albums`")
    print(f">>> Attempting to Insert {len(albums_data)} documents in Albums collection")
    
    # Inserting Albums Data
    albums = db["Albums"]

    # Insert many documents into the collection
    result = albums.insert_many(albums_data)
    print(result)

    print(">>> Inserting documents in Albums collection successful")

    # Creating new Collection `Artists`
    print(">>> Creating Collection `Artists`")
    print(f">>> Attempting to Insert {len(artists_data)} documents in Artists collection")
    
    # Inserting Artists 
    artists = db["Artists"]

    # Insert many documents into the collection
    result = artists.insert_many(artists_data)
    print(result)

    print(">>> Inserting documents in Artists collection successful")

    # Print the inserted document IDs
    print(">>> Safely closing the conection with MongoDB Server!")
    client.close()
    # Importing into Neo4J

