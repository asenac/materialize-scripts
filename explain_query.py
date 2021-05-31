#!/usr/bin/env python3

import psycopg2
import itertools
import sys
from dataflow_parser import generate_graph

conn = psycopg2.connect(
    host="localhost",
    port=6875,
    database="materialize",
    user="materialize")

with conn.cursor() as cursor:
    for arg in sys.argv[1:]:
        for mode in ["raw plan for", "decorrelated plan for", "", "typed plan for"]:
            query = "explain " + mode + " " + arg
            cursor.execute(query)
            row = cursor.fetchone()
            generate_graph(row[0].splitlines(), query)

conn.close()
