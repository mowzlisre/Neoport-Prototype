
a = """
CREATE (track:Track {
            id: $id,
            name: $name,
            track_number: $track_number,
            disc_number: $disc_number,
            explicit: $explicit,
            danceability: $danceability,
            energy: $energy,
            key: $key,
            loudness: $loudness,
            mode: $mode,
            speechiness: $speechiness,
            acousticness: $acousticness,
            instrumentalness: $instrumentalness,
            liveness: $liveness,
            valence: $valence,
            tempo: $tempo,
            duration_ms: $duration_ms,
            time_signature: $time_signature,
            year: $year,
            release_date: $release_date,
            artist_names: $artist_names
        })
        MERGE (_album:Album {
            album_name: $album,
            album_id: $album_id
        })
        WITH track, _album
        MERGE (_album)-[:ContainsTrack]->(track)

        MERGE (_artist0:Artist {{id: "{item["artist_ids"][index]}", artist_name: "{item["artist_names"].split(',')[index]}"}})
            WITH _artist0, _album
            MERGE (_artist0)-[:Contributed]->(_album)
        

        MERGE (_artist1:Artist {{id: "{item["artist_ids"][index]}", artist_name: "{item["artist_names"].split(',')[index]}"}})
            WITH _artist1, _album
            MERGE (_artist1)-[:Contributed]->(_album)


        MERGE (_artist2:Artist {{id: "{item["artist_ids"][index]}", artist_name: "{item["artist_names"].split(',')[index]}"}})
            WITH _artist2, _album
            MERGE (_artist2)-[:Contributed]->(_album)

        
"""