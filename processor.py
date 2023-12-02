import re, ast, time

def preprocess(data):
    albums_data = []
    artists_data = []
    existing_artist_ids = set()  # Use a set for faster lookup
    print(">>> Preprocessor Phase 1 - Identifying and evaluating the native data types")
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
    print(">>> Preprocessor Phase 1 - Completed!")
    time.sleep(2)
    print(">>> Preprocessor Phase II - Identifying and eliminating duplicates")          
    albums_data = [dict(t) for t in {tuple(d.items()) for d in albums_data}]
    print(">>> Preprocessor Phase 1I - Completed!")
    time.sleep(2)
    return data, albums_data, artists_data

def postprocess(data):
    for row in data:
        del row["artists"]
        del row["artist_ids"]
        del row["album"]
    return data