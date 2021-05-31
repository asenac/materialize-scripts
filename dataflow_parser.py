#!/usr/bin/env python3

import sys
import re



def generate_graph(lines, graph_label):
    nodes = []
    edges = set()
    current_node = None

    print (lines)

    for line in lines:
        if line.startswith('%'):
            current_node = dict()
            current_node['name'] = line[:line.find(' ')]
            current_node['text'] = line[line.find('=', 1) + 1:]
        elif len(line) == 0:
            nodes.append(current_node)
            current_node = None
        else:
            current_node['text'] += '\n' + line

    if current_node is not None:
        nodes.append(current_node)

    for n in nodes:
        for d in re.findall(r'%\d+', n['text']):
            edges.add((d, n['name']))

    def node(x):
        return x.replace('%', 'node')
    def label(l):
        return l.strip(' \n').replace('\n', '\\l').replace('"', '\\"').replace('|', '').replace('>', '\\>').replace('<', '\\<') + '\\l'

    sys.stdout.write('digraph G {\n    label="%s"\n' % (label(graph_label)))

    for n in nodes:
        sys.stdout.write('    %s [shape = record, label="%s"]\n' % (node(n['name']), label(n['text'])))

    for e in edges:
        sys.stdout.write('    %s -> %s [label = "%s"]\n' % (node(e[0]), node(e[1]), label(e[0])))

    sys.stdout.write('}\n')


if __name__ == '__main__':
    lines = sys.stdin.readlines()
    generate_graph(lines, 'stdin')
