#!/bin/sh

TEMP=/tmp/magic-pager-$RANDOM.txt
tee $TEMP

extract_graph() {
    sed -n "/^ digraph G {\s*+$/,/^ }\s*+$/p" $1 | sed -e 's/+$//' | sed -e 's/\s*$//' | sed -e 's/^ //' | csplit --prefix $1- --suffix-format "%04d.dot" --elide-empty-files - '/^digraph G {$/' '{*}'
    [ -f $1-*.dot ] && for i in $1-*.dot; do
        dot -Tsvg -O $i
    done
}

extract_explain_graph() {
    sed -n "/^ %0 = .*+$/,/^($/p" $1 | sed -e 's/+$//' | sed -e 's/\s*$//' | sed -e 's/^ //' | grep -v "^(" | csplit --prefix $1- --suffix-format "%04d.dot" --elide-empty-files - '/^%0 =/' '{*}'
    [ -f $1-*.dot ] && for i in $1-*.dot; do
        cat $i | ~/dev/materialize-scripts/dataflow_parser.py | dot -Tsvg > $i.svg
    done
}

extract_graph $TEMP
extract_explain_graph $TEMP
[ -f $TEMP*.svg ] && eog -w $TEMP*.svg

rm -f $TEMP*
