import re, ast, time
from collections import defaultdict

def preprocess(data):
    albums_data = []
    artists_data = []
    existing_artist_ids = set()  # Use a set for faster lookup
    ab_tr_rel = []
    at_ab_rel = []
    print(">>> Preprocessor Phase 1 - Identifying and evaluating the native data types")
    for row in data:
        row['artists'] = ast.literal_eval(row['artists'])
        row['artist_ids'] = ast.literal_eval(row['artist_ids'])
        
        albums_data.append({
            "name": row['album'],
            "_id": row["album_id"],
            "artists_ids" : '-'.join(row['artist_ids'])
        })
        rel_1 = {
            "album_id" : row["album_id"],
            "track_id" : row["id"]
        }
        ab_tr_rel.append(rel_1)

        for index, artist in enumerate(row['artist_ids']):
            if artist not in existing_artist_ids:
                artists_data.append({
                    "name": row['artists'][index],
                    "_id": artist
                })
                existing_artist_ids.add(artist) 
                rel_2 = {
                    "artist_id" : artist,
                    "album_id" : row["album_id"]
                }
            at_ab_rel.append(rel_2)
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
        del row["artists"]
        del row["artist_ids"]
        del row["album"]

    print(">>> Preprocessor Phase 1 - Completed!")
    time.sleep(2)
    print(">>> Preprocessor Phase II - Identifying and eliminating duplicates")          
    albums_data = [dict(t) for t in {tuple(d.items()) for d in albums_data}]   
    grouped = defaultdict(list)
    for item in albums_data:
        grouped[(item['name'], item['_id'])].extend(item['artists_ids'].split("-"))

    # Create a new list of merged dictionaries
    albums_data = [
        {"name": name, "_id": _id, "artists_ids": list(set(artists))}
        for (name, _id), artists in grouped.items()
    ]
    artists_data = [dict(t) for t in {tuple(d.items()) for d in artists_data}]
       
    ab_tr_rel = [dict(t) for t in {tuple(d.items()) for d in ab_tr_rel}]        
    at_ab_rel = [dict(t) for t in {tuple(d.items()) for d in at_ab_rel}]
    print(">>> Preprocessor Phase 1I - Completed!")
    time.sleep(2)
    import csv
    def write_to_csv(data, filename):
        keys = data[0].keys()  # Assuming all dictionaries in the list have the same keys
        with open(filename, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

    # Call the function to write 'ab_tr_rel' data to a CSV file
    write_to_csv(ab_tr_rel, 'ab_tr_rel.csv')
    write_to_csv(at_ab_rel, 'at_ab_rel.csv')
    return data, albums_data, artists_data, ab_tr_rel, at_ab_rel
