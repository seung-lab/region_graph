#!/bin/bash
yell() { echo "$0: $*" >&2;  }
die() { yell "$*"; exit 111;  }
bail() { yell "$*"; exit 0; }
try() { "$@" || die "cannot $*";  }
try_bail() { "$@" && touch /tmp/finished.txt && bail "skip $*"; }

i=$1
S3PATH="s3://seunglab/region_graph_data"

try_bail aws s3 cp $S3PATH/done/"$i".txt .

cd /mnt/data01
rm *.tar.bz2
rm *.txt


try aws s3 cp $S3PATH/edges/edges_"$i".tar.bz2 .
try tar --strip-components=1 -jxvf edges_"$i".tar.bz2 >& tar_output.log

try seq 0 7|parallel -j 8 --halt 2 julia combine_edges.jl input_{}.txt output_{}.txt

rm more_edges_"$i".txt
touch more_edges_"$i".txt

for j in {0..7}; do try cat output_${j}.txt >> more_edges_"$i".txt; done

try aws s3 cp more_edges_"$i".txt $S3PATH/more_edges/

try touch "$i".txt
try aws s3 cp "$i".txt $S3PATH/done/

rm *.tar.bz2
rm *.txt
