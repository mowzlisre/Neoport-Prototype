from support import *
from batchProcessor import batch_process
def importDB(data, albums_data, artists_data, ab_tr_rel, at_ab_rel):
    query1 = """
                UNWIND $nodes AS node
                CREATE (_album:Album {
                    id: node.id,
                    name: node.name
                }) 
            """
    query2 = """
                UNWIND $nodes AS node
                CREATE (_artist:Artist {
                    id: node.id,
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

    print(">>> Inserting Artist nodes")
    batch_process(artists_data[:1000], query1, False)
    print(">>> Inserting Album nodes")
    batch_process(albums_data[:1000], query2, False)
    print(">>> Inserting Track nodes")
    batch_process(data[:1000], query3, False)
    print(">>> Inserting Albums-Track Relationships")
    batch_process(ab_tr_rel[:10], '', False)
    print(">>> Inserting Artists-Albums Relationships")
    batch_process(at_ab_rel[:10], '', False)

