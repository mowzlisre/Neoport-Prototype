NEO_URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "12345678"
CHUNK = 50000

MGDB_URI = 'mongodb://localhost:27017/'
MGDB_DBN = 'TracksDB'


def generate_number_sequence(n):
    result = []
    div = CHUNK
    while div <= n:
        result.append(div)
        div += CHUNK
    if result[-1] != n:
        result.append(n)
    return result
