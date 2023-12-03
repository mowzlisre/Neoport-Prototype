import pymongo

def client(query, coll, type):

    # Establishing a connection to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB URI

    # Selecting a database
    database = client["TracksDB"]  

    # Selecting a collection
    collection = database[coll]  

    # Executing the query and fetching the result
    if type == 'aggr':
        result = list(collection.aggregate(query))

    # Closing the MongoDB connection
    client.close()
    return result
