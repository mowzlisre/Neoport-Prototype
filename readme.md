## Qualitative Comparison of MongoDB vs Neo4J
### Team 5 - Gourab Mukherjee, Monica Muniraj, Mowzli Sre Mohan Dass

### Pre-requisite before execution
- Import the `neo4j.dump` file to create a new database with Nodes and Relationships and start the Neo4J Database Server and MongoDB Server
- Install the pip dependencies
    `pip install neo4j pymongo` or `pip3 install neo4j pymongo`
- Once the DB servers are started, run the main.py file
    `main.py` or `python main.py` or `python3 main.py`
- The main.py file does the following:
    - Data Preprocessing from the csv file
    - Insertion of data into Neo4j database (commented out, it takes huge time to insert nodes and relationships together in the database if the dataset is large)
    - Insertion of data into the MongoDB.
        - Automatically create the Database `TracksDB`
        - Also creates the Collections `Tracks`, `Albums`, `Artists`
    - Evaluates the pre-defined queries from `$root/client/queries.py` and shows performance insights

Currently we are working on scaling the Neo4J database since the insertion of Nodes along with the Relationship is very costly and takes hours to import the data unlike MongoDB. With intermediate level queries, Neo4J did outperformed MongoDB even before exploring with the Traversal techniques (which are not in the scope of the dataset, but still can be implemented)

Once Neo4J is scaled to larger datasets, new `neo4j.dump` with more than 1M nodes will be supplied and the same above pre-requisite will be followed for the execution to support our Research Analysis

