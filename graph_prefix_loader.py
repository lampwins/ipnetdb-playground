import json
import json
import multiprocessing
from neo4j import GraphDatabase

# Connect to Neo4j database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "zwn@mty2nty_MER5wqc"))

def load_objects(objects):
    with driver.session() as session:
        session.run("UNWIND $nodes_and_rels as n "
                    "MERGE (a:Prefix {prefix: n.prefix, rpki_status: n.rpki_status}) "
                    "WITH a, n.prefix_origins as origins "
                    "UNWIND origins as origin "
                    "MERGE (b:AS {asn: origin}) "
                    "CREATE (b)-[:ADVERTISES]->(a)", nodes_and_rels=objects)

if __name__ == '__main__':
    # Number of processes to use
    num_processes = 10
    objects = []

    # Read the JSON objects from the file
    with open('prefix_db.json', 'r') as f:
        for line in f:
            line = json.loads(line)
            if all(k in line for k in ["prefix", "rpki_status", "prefix_origins", "allocation_status"]) and line["allocation_status"] == "allocated":
                objects.append(line)

    # Split the objects into chunks for each process
    chunk_size = len(objects) // num_processes
    chunks = [objects[i:i+chunk_size] for i in range(0, len(objects), chunk_size)]

    ## Create a list of processes
    #processes = []
    #for chunk in chunks:
    #    p = multiprocessing.Process(target=load_objects, args=(chunk,))
    #    processes.append(p)
    #    p.name = "asn-loader"
    #    p.start()
#
    ## Wait for all processes to finish
    #for p in processes:
    #    p.join()

    i = 1
    for chunk in chunks:
        print("Loading chunk " + str(i) + " of " + str(len(chunks)))
        load_objects(chunk)
        i += 1
