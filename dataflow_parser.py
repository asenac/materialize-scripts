#!/usr/bin/env python3

import sys
import re

nodes = []
edges = set()

current_node = None

while 1:
    line = sys.stdin.readline()
    if not line:
        break

    if line.startswith(' %'):
        current_node = dict()
        current_node['name'] = line[1:line.find(' ', 1)]
        current_node['text'] = line[line.find('=', 2) + 1:]
    elif line == '\n':
        nodes.append(current_node)
    else:
        current_node['text'] += line

for n in nodes:
    for d in re.findall(r'%\d+', n['text']):
        edges.add((d, n['name']))

def node(x):
    return x.replace('%', 'node')
def label(l):
    return l.strip(' \n').replace('\n', '\\l').replace('"', '\\"').replace('|', '').replace('>', '\\>').replace('<', '\\<') + '\\l'

sys.stdout.write('digraph G {\n')

for n in nodes:
    sys.stdout.write('    %s [shape = record, label="%s"]\n' % (node(n['name']), label(n['text'])))

for e in edges:
    sys.stdout.write('    %s -> %s [label = "%s"]\n' % (node(e[0]), node(e[1]), label(e[0])))

sys.stdout.write('}\n')
