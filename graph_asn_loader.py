import json
import json
import multiprocessing
from neo4j import GraphDatabase

# Connect to Neo4j database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "zwn@mty2nty_MER5wqc"))

def load_objects(objects):
    with driver.session() as session:
        session.run("UNWIND $nodes_and_rels as n "
                    "MERGE (a:AS {asn: n.asn, name: n.name, entity: n.entity}) "
                    "WITH a, n.peers as peers "
                    "UNWIND peers as peer "
                    "MERGE (b:AS {asn: peer}) "
                    "CREATE (a)-[:PEER]->(b)", nodes_and_rels=objects)

if __name__ == '__main__':
    # Number of processes to use
    num_processes = 12

    # Read the JSON objects from the file
    with open('asn_nodes.json', 'r') as f:
        objects = [json.loads(line) for line in f]

    # Split the objects into chunks for each process
    chunk_size = len(objects) // num_processes
    chunks = [objects[i:i+chunk_size] for i in range(0, len(objects), chunk_size)]

    # Create a list of processes
    processes = []
    for chunk in chunks:
        p = multiprocessing.Process(target=load_objects, args=(chunk,))
        processes.append(p)
        p.name = "asn-loader"
        p.start()

    # Wait for all processes to finish
    for p in processes:
        p.join()

