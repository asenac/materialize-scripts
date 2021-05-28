#!/usr/bin/env python3

import psycopg2
import itertools
import sys
import pandas as pd
import math

conn = psycopg2.connect(
    host="localhost",
    port=6875,
    database="materialize",
    user="materialize")

the_query = '''
            with
                 dataflow_channels as (select distinct c.id, o.address, source_node, source_port, target_node, target_port from mz_dataflow_channels c, mz_dataflow_operator_addresses o where o.id = c.id),
                 dataflow_operators as (select distinct name, address, n.id from mz_dataflow_operators n, mz_dataflow_operator_addresses a where n.id = a.id)
            select c.address[1] as dataflow_id,
                   c.id as channel_id, c.address as channel_address, source_node, source_port, target_node, target_port, sum(sent) as sent, sum(received) as received,
                   (case when src.name is null then 'input_' || source_port else src.name end) as source_name, src.address as source_address, src.id as source_id,
                   (case when dst.name is null then 'output_' || target_port else dst.name end) as target_name, dst.address as target_address, dst.id as target_id
            from
                dataflow_channels c
                left join mz_message_counts mc on c.id = mc.channel
                left join dataflow_operators src
                    on c.address = src.address[1:list_length(src.address) - 1]
                    and c.source_node = src.address[list_length(src.address)]
                left join dataflow_operators dst
                    on c.address = dst.address[1:list_length(dst.address) - 1]
                    and c.target_node = dst.address[list_length(dst.address)]
                group by c.id, c.address, source_node, source_port, target_node, target_port,
                    src.name, src.address, src.id,
                    dst.name, dst.address, dst.id;
            '''

df = pd.read_sql_query(the_query, con=conn)

def as_list(a):
    return a[1:-1].split(',')
def address_to_str(a):
    return 'addr_' + '_'.join(as_list(a))

class Node:
    def __init__(self, address, name, id_):
        self.address = address
        self.name = name
        self.id_ = id_

class Edge:
    def __init__(self, src, dst, sent):
        self.src = src
        self.dst = dst
        self.sent = sent

class Region:
    def __init__(self, address):
        self.address = address
        self.name = ''
        self.subregions = []
        self.nodes = {}

    def set_name(self, name):
        self.name = name

for state, dataflow in df.groupby(by='dataflow_id'):
    regions = {x: Region(x) for x in set([x for x, y in df.groupby(by='channel_address')])}
    root_region = '{' + str(state) + '}'
    regions[root_region] = Region(root_region)

    edges = []

    for region_addr, region_frame in dataflow.groupby(by='channel_address'):
        region = regions[region_addr]

        # parent region
        parent_region = as_list(region_addr)[:-1]
        if parent_region:
            regions['{' + ','.join(parent_region) + '}'].subregions.append(region_addr)

        for index, n in region_frame.iterrows():
            def add_node(prefix):
                address = n[prefix + '_address']
                addr_str = ''
                if address is not None:
                    addr_str = address_to_str(address)
                    if address in regions:
                        regions[address].set_name(n[prefix + '_name'])
                        addr_str += '_' + ('source' if prefix == 'target' else 'target') + '_' + str(n[prefix + '_port'])
                        return addr_str
                else:
                    addr_str = address_to_str(region_addr) + '_' + prefix + '_' + str(n[prefix + '_port'])
                region.nodes[addr_str] = Node(addr_str, n[prefix + '_name'], n[prefix + '_id'])
                return addr_str
            s = add_node('source')
            d = add_node('target')
            edges.append(Edge(s, d, n['sent']))


    def print_region(region_addr):
        region = regions[region_addr]
        sys.stdout.write('  subgraph cluster_%s {\n    graph[style=dotted]\n    label="%s"\n' % (address_to_str(region_addr), region.name))

        for addr, node in region.nodes.items():
            prefix = '%s: ' % (int(node.id_)) if not math.isnan(node.id_) else ''
            sys.stdout.write('    node_%s [label="%s%s"];\n' % (node.address, prefix, node.name))

        for subregion in region.subregions:
            print_region(subregion)

        sys.stdout.write('  }\n')

    sys.stdout.write('digraph G {\n  compound=true\n')
    print_region(root_region)

    for e in edges:
        label = 'sent %s' % (int(e.sent)) if not math.isnan(e.sent) else ''
        sys.stdout.write('    node_%s -> node_%s [label = "%s"];\n' % (e.src, e.dst, label))
    sys.stdout.write('}\n')

conn.close()
