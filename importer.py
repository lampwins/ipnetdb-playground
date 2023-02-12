import json
from ipaddress import IPv4Network, IPv6Network

from cog.torque import Graph
from maxminddb.const import MODE_AUTO
from maxminddb.reader import Reader


class IterableCallbackReader(Reader):
    """
    Implements `iterate_search_tree` on the base `maxminddb.reader.Reader`

    Accepts a callback callabe to handle each data record
    """

    def iterate_search_tree(self, data_callback=lambda: None, ip_version=6):
        if ip_version == 6:
            max_bit_depth = 128
        elif ip_version == 4:
            max_bit_depth = 32
        else:
            raise ValueError('Invalid ip version')

        start_node = self._start_node(max_bit_depth)

        self._iterate_search_tree(
            data_callback=data_callback,
            start_node=start_node,
            bit_depth=1,
            max_bit_depth=max_bit_depth,
            decimal_ip=0,
        )

    def _iterate_search_tree(self, data_callback=lambda: None, start_node=0, bit_depth=1, max_bit_depth=128, decimal_ip=0):
        for bit in [0, 1]:
            node = self._read_node(start_node + bit_depth, bit)
            if node == self._metadata.node_count:
                continue

            if bit:
                decimal_ip = decimal_ip | 1 << (max_bit_depth - bit_depth)

            if node <= self._metadata.node_count:
                self._iterate_search_tree(
                    data_callback=data_callback,
                    start_node=start_node,
                    bit_depth=bit_depth + 1,
                    max_bit_depth=max_bit_depth,
                    decimal_ip=decimal_ip
                )
            else:
                mask = bit_depth + bit
                data = self._resolve_data_pointer(node)
                data_callback(decimal_ip, mask, data)

    def data_generator(self):
        if self._metadata.ip_version == 4:
            start_node = self._start_node(32)
            start_network = IPv4Network((0, 0))
        else:
            start_node = self._start_node(128)
            start_network = IPv6Network((0, 0))

        search_nodes = [(start_node, start_network)]
        while search_nodes:
            node, network = search_nodes.pop()

            if network.version == 6:
                naddr = network.network_address
                if naddr.ipv4_mapped or naddr.sixtofour:
                    # skip IPv4-Mapped IPv6 and 6to4 mapped addresses, as these are
                    # already included in the IPv4 part of the tree below
                    continue
                elif int(naddr) < 2 ** 32 and network.prefixlen == 96:
                    # once in the IPv4 part of the tree, switch to IPv4Network
                    ipnum = int(naddr)
                    mask = network.prefixlen - 128 + 32
                    network = IPv4Network((ipnum, mask))

            subnets = list(network.subnets())
            for bit in (0, 1):
                next_node = self._read_node(node, bit)
                subnet = subnets[bit]

                if next_node > self._metadata.node_count:
                    data = self._resolve_data_pointer(next_node)
                    yield data
                elif next_node < self._metadata.node_count:
                    search_nodes.append((next_node, subnet))


def print_record(decimal_ip, mask, data):
    print(decimal_ip, " - ", mask , " - ", data["allocation"])


def print_asn_record(subnet, data):
    print(data)


#if __name__ == "__main__":
#
#    #with IterableCallbackReader('ipnetdb_prefix_latest.mmdb', MODE_AUTO) as reader:
#    #    reader.iterate_search_tree(print_record, 4)
#
#    g = Graph("asns")
#
#    with IterableCallbackReader('ipnetdb_asn_latest.mmdb', MODE_AUTO) as reader:
#        for asn in reader.asn_generator():
#            if asn["in_use"]:
#
#                _as = str(asn["as"])
#
#                peers = asn.get("peers", None)
#                if peers is None:
#                    continue
#
#                v4_addresses = asn.get("ipv4_prefixes", None)
#                if v4_addresses is None:
#                    continue
#
#                v6_addresses = asn.get("ipv6_prefixes", None)
#                if v6_addresses is None:
#                    continue
#
#                # peers
#                for peer in peers:
#                    g.put(_as, "peers_with", str(peer))
#
#                # advertisements
#                for v4_address in v4_addresses:
#                    g.put(_as, "advertises", v4_address)
#                # advertisements
#                for v6_address in v6_addresses:
#                    g.put(_as, "advertises", v4_address)
#
#    g.v().tag("from").out("peers_with").tag("to").view("peers_with").url


if __name__ == '__main__':

    with IterableCallbackReader('ipnetdb_prefix_latest.mmdb', MODE_AUTO) as reader:
        with open("prefix_db.json", 'w') as f:
            for item in reader.data_generator():
                json.dump(item, f)
                f.write('\n')
