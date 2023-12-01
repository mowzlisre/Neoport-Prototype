
URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "12345678"

def generate_number_sequence(n):
    result = []
    div = 50000
    while div <= n:
        result.append(div)
        div += 50000
    if result[-1] != n:
        result.append(n)
    return result