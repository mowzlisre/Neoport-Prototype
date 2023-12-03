import csv, neo4jProcessor, mongoProcessor, time
from processor import preprocess


if __name__ == "__main__":
    init = time.time()
    print(">>> Automated Data Insertion - Neo4J & MongoDB")
    # Data Loading
    print(">>> Loading the CSV file")
    with open('sample.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        data = list(reader)
        print(">>> Loading CSV file succesful")

    # Data Preprocessing
    print(">>> Initializing the Data Preprocessor")
    pr_init = time.time()
    data, albums_data, artists_data, ab_tr_rel, at_ab_rel = preprocess(data)
    pr_end = time.time()
    print(f">>> Data Preprocessing completed in {pr_end-pr_init-4:.2f}s")

    # # Import Data to Neo4J Database
    # print(">>> Initializing the Data Import to Neo4j")
    # nj_init = time.time()
    # neo4jProcessor.importDB(data, albums_data, artists_data, ab_tr_rel, at_ab_rel)
    # nj_end = time.time()
    # print(f">>> {len(data)} Tracks, {len(albums_data)} Albums, {len(artists_data)} Artists nodes and relationships imported succesfully in {nj_end-nj_init:.2f}s")

    # Import Data to MongoDB Database
    print(">>> Initializing the Data Import to MongoDB")
    mg_init = time.time()
    mongoProcessor.importDB(data, albums_data, artists_data)
    mg_end = time.time()
    print(f">>> {len(data)} Tracks, {len(albums_data)} Albums, {len(artists_data)} Artists documents imported succesfully in {mg_end-mg_init:.2f}s")
    end = time.time()
    print(f">>> Automated Data Insertion Completed in {end-init-4:.2f}s")


    
