#!/usr/bin/env python3

import tornado.ioloop
import tornado.web
import psycopg2
import itertools
import sys
import json
import pandas as pd
import math

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

def get_dataflow(dataflow_id):
    with psycopg2.connect(
        host="localhost",
        port=6875,
        database="materialize",
        user="materialize") as conn:
        the_query = 'select * from metadataflow where dataflow_id = %s' % (dataflow_id)

        df = pd.read_sql_query(the_query, con=conn)

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

            return (root_region, regions, edges)
        return (None, None, None)

def get_dataflow_graph(dataflow_id):
    root_region, regions, edges = get_dataflow(dataflow_id)

    def print_region(region_addr):
        region = regions[region_addr]
        output = '  subgraph cluster_%s {\n    graph[style=dotted]\n    label="%s"\n' % (address_to_str(region_addr), region.name)

        for addr, node in region.nodes.items():
            prefix = '%s: ' % (int(node.id_)) if node.id_ is not None and not math.isnan(node.id_) else ''
            output += '    node_%s [label="%s%s"];\n' % (node.address, prefix, node.name)

        for subregion in region.subregions:
            output += print_region(subregion)

        output += '  }\n'
        return output

    output = ""

    output += 'digraph G {\n  compound=true\n'
    output += print_region(root_region)

    for e in edges:
        label = 'sent %s' % (int(e.sent)) if e.sent is not None and not math.isnan(e.sent) else ''
        output += '    node_%s -> node_%s [label = "%s", id = "edge_%s_%s"];\n' % (e.src, e.dst, label, e.src, e.dst)
    output += '}\n'
    return output

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html")

class DataflowHandler(tornado.web.RequestHandler):
    def get(self, dataflow_id):
        graph = get_dataflow_graph(dataflow_id)
        self.finish(graph)

class DataflowEdgesHandler(tornado.web.RequestHandler):
    def get(self, dataflow_id):
        _, _, edges = get_dataflow(dataflow_id)
        o = {}
        for e in edges:
            edge_id = "edge_%s_%s" % (e.src, e.dst)
            label = 'sent %s' % (int(e.sent)) if e.sent is not None and not math.isnan(e.sent) else ''
            o[edge_id] = label
        self.finish(json.dumps(o))

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/dataflow/(.*)", DataflowHandler),
        (r"/dataflow-edges/(.*)", DataflowEdgesHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()

