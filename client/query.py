queries = [
    {
        "description": "Aggregate average danceability and energy values of high-energy, low-acousticness music tracks, organizing by year and presented in descending order",
        "mongo": [
            {"$match": {"energy": {"$gt": 0.8}, "acousticness": {"$lt": 0.2}}},
            {
                "$group": {
                    "_id": "$year",
                    "avgDanceability": {"$avg": "$danceability"},
                    "avgEnergy": {"$avg": "$energy"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "year": "$_id",
                    "avgDanceability": 1,
                    "avgEnergy": 1,
                }
            },
            {"$sort": {"year": -1}},
        ],
        "neo": """
            MATCH (t:Track)
            WHERE t.energy > 0.8 AND t.acousticness < 0.2
            WITH t.year AS year, 
                AVG(t.danceability) AS avgDanceability, 
                AVG(t.energy) AS avgEnergy
            WITH year, avgDanceability, avgEnergy
            RETURN year AS year,
                avgDanceability AS avgDanceability,
                avgEnergy AS avgEnergy
            ORDER BY year DESC
        """,
    },
    {
        "description": "Extract attributes (duration, loudness, liveness, acousticness) from music tracks in a MongoDB dataset, filtering for tracks with durations between 3 to 5 minutes, low loudness, high liveness, and minimal acousticness",
        "mongo": [
            {
                "$match": {
                    "duration_ms": {"$gte": 180000, "$lte": 300000},
                    "loudness": {"$lt": -5},
                    "liveness": {"$gt": 0.3},
                    "acousticness": {"$lt": 0.05},
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "name": 1,
                    "duration_ms": 1,
                    "loudness": 1,
                    "liveness": 1,
                    "acousticness": 1,
                }
            },
        ],
        "neo": """
            MATCH (t:Track)
            WHERE t.duration_ms >= 180000 AND t.duration_ms <= 300000
            AND t.loudness < -5
            AND t.liveness > 0.3
            AND t.acousticness < 0.05
            RETURN t._id AS _id,
                t.name AS name,
                t.duration_ms AS duration_ms,
                t.loudness AS loudness,
                t.liveness AS liveness,
                t.acousticness AS acousticness
        """,
    },
    {
        "description": "Extract tracks linked to specific artist IDs, narrow down the selection to the years between 1980 and 2000, and reshape the output to emphasize crucial track details",
        "mongo": [
            {"$match": {"artists_ids": {"$in": ["0vaHBG5ZaNrZipcbbM6bCn"]}}},
            {"$group": {"_id": None, "albumIds": {"$push": "$_id"}}},
            {
                "$lookup": {
                    "from": "Tracks",
                    "localField": "albumIds",
                    "foreignField": "album_id",
                    "as": "tracksResult",
                }
            },
            {"$unwind": "$tracksResult"},
            {"$match": {"tracksResult.year": {"$gte": 1980, "$lte": 2000}}},
            {"$replaceRoot": {"newRoot": "$tracksResult"}},
        ],
        "neo": """
            MATCH (a:Artist {id: '0vaHBG5ZaNrZipcbbM6bCn'})-[:CONTRIBUTED]->(al:Album)-[:CONTAINS]->(t:Track)
            WHERE t.year >= 1980 AND t.year <= 2000
            RETURN t
        """,
    },
    {
        "description": "Pair track names with their corresponding album names , demonstrating a process of merging and projecting specific fields",
        "mongo": [
            {
                "$lookup": {
                    "from": "Albums",
                    "localField": "album_id",
                    "foreignField": "_id",
                    "as": "album",
                }
            },
            {"$unwind": "$album"},
            {
                "$project": {
                    "_id": 1,
                    "track_name": "$name",
                    "album_name": "$album.name",
                }
            },
        ],
        "neo": """
            MATCH (t:Track)
            MATCH (t)<-[:CONTAINS]-(al:Album)
            RETURN t._id AS _id, t.name AS track_name, al.name AS album_name
        """,
    },
    {
        "description": "Extract album IDs, names, and pertinent track statistics belonging to a specific artist between 1980 and 2000",
        "mongo": [
            {"$match": {"artist_ids": {"$in": ["39gp1NxfhmLEyvNggMH4xg"]}}},
            {
                "$lookup": {
                    "from": "Albums",
                    "localField": "album_id",
                    "foreignField": "_id",
                    "as": "album",
                }
            },
            {"$unwind": "$album"},
            {"$match": {"year": {"$gte": 1980, "$lte": 2000}}},
            {
                "$group": {
                    "_id": "$album._id",
                    "album_name": {"$first": "$album.name"},
                    "total_tracks": {
                        "$sum": {"$cond": [{"$eq": [1, 1]}, 1, 0]}
                    },  # Inefficient total_tracks calculation
                    "avg_energy": {"$avg": "$energy"},
                    "avg_danceability": {"$avg": "$danceability"},
                    "max_tempo": {"$max": "$tempo"},
                    "min_tempo": {"$min": "$tempo"},
                }
            },
            {"$sort": {"total_tracks": -1}},
            {"$limit": 5},
        ],
        "neo": """
                MATCH (t:Track)<-[:CONTAINS]-(al:Album)
                WHERE t.artist_id = "0vaHBG5ZaNrZipcbbM6bCn" AND t.year >= 1980 AND t.year <= 2000
                WITH al, COUNT(t) AS total_tracks, COLLECT(t) AS tracks
                RETURN 
                al._id AS album_id,
                al.name AS album_name,
                total_tracks,
                REDUCE(s = 0, track IN tracks | s + track.energy) / toFloat(total_tracks) AS avg_energy,
                REDUCE(s = 0, track IN tracks | s + track.danceability) / toFloat(total_tracks) AS avg_danceability,
                MAX([track in tracks | track.tempo])[0] AS max_tempo,
                MIN([track in tracks | track.tempo])[0] AS min_tempo
                ORDER BY total_tracks DESC
                LIMIT 5
            """,
    },
]
