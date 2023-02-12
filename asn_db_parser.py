import json


def transformer(data):
    return {
        "asn": str(data["as"]),
        "peers": [str(x) for x in data.get("peers", [])],
        "v4_advertisements": data.get("ipv4_prefixes", []),
        "v6_advertisements": data.get("ipv6_prefixes", []),
        "name": data["name"],
        "entity": data["entity"],
    }


with open("asn_db.json", 'r') as fi:
    with open("asn_nodes.json", 'w') as fo:
        for line in fi:
            data = json.loads(line)
            if any(k not in data for k in ["peers", "ipv4_prefixes", "ipv6_prefixes", "name", "entity", "in_use"]) or not data["in_use"]:
                continue
            transformed_data = transformer(data)
            json.dump(transformed_data, fo)
            fo.write('\n')