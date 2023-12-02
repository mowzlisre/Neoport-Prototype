from support import *
from batchProcessor import batch_process
def importDB(data, albums_data, artists_data, ab_tr_rel, at_ab_rel):
    query1 = """
                UNWIND $nodes AS node
                CREATE (_album:Album {
                    id: node._id,
                    name: node.name
                }) 
            """
    query2 = """
                UNWIND $nodes AS node
                CREATE (_artist:Artist {
                    id: node._id,
                    name: node.name
                })
            """
    query3 = """
                UNWIND $nodes AS node
                CREATE (track:Track {
                    id: node.id,
                    name: node.name,
                    track_number: node.track_number,
                    disc_number: node.disc_number,
                    explicit: node.explicit,
                    danceability: node.danceability,
                    energy: node.energy,
                    key: node.key,
                    loudness: node.loudness,
                    mode: node.mode,
                    speechiness: node.speechiness,
                    acousticness: node.acousticness,
                    instrumentalness: node.instrumentalness,
                    liveness: node.liveness,
                    valence: node.valence,
                    tempo: node.tempo,
                    duration_ms: node.duration_ms,
                    time_signature: node.time_signature,
                    year: node.year,
                    release_date: node.release_date
                })
            """
    query4 = """
                UNWIND $nodes AS node
                CREATE (a:Album {id: node.album_id})-[:CONTAINS_TRACKS]->(t:Track {id: node.track_id})
            """
    query5 = """
                UNWIND $nodes AS node
                CREATE (a:Artist {id: node.artist_id})-[:CONTRIBUTED_TO]->(t:ALBUM {id: node.album_id})
            """
    print(">>> Inserting Artist nodes")
    batch_process(artists_data, query1)
    print(">>> Inserting Album nodes")
    batch_process(albums_data, query2)
    print(">>> Inserting Track nodes")
    batch_process(data, query3)
    print(f">>> Inserting {len(ab_tr_rel)} Albums-Track Relationships")
    batch_process(ab_tr_rel, query4)
    print(f">>> Inserting {len(at_ab_rel)} Artists-Albums Relationships")
    batch_process(at_ab_rel, query5)

