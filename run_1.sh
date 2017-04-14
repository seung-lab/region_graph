#!/bin/bash
yell() { echo "$0: $*" >&2;  }
die() { yell "$*"; exit 111;  }
bail() { yell "$*"; exit 0; }
try() { "$@" || die "cannot $*";  }
try_bail() { "$@" && touch /tmp/finished.txt && bail "skip $*"; }

i=$1
j=$2
k=$3

cd /mnt/data01

try_bail aws s3 cp s3://seunglab/region_graph_data/done/"$i"_"$j"_"$k".txt .

try julia cut_segments.jl aff $i $j $k
try julia cut_segments.jl seg $i $j $k

try julia rg_mmap.jl aff_chunk seg_chunk $i $j $k
try find . -name '*.txt' -print > /tmp/test.manifest
try tar jcvf incomplete_edges_"$i"_"$j"_"$k".tar.bz2 --files-from /tmp/test.manifest
try aws s3 cp incomplete_edges_"$i"_"$j"_"$k".tar.bz2 s3://seunglab/region_graph_data/incomplete_edges/
try aws s3 cp rg_volume_"$i"_"$j"_"$k".in s3://seunglab/region_graph_data/region_graphs/

rm *.tar.bz2
find . -name '*.txt' | xargs rm -v

try julia rg_mmap.jl aff_chunk seg_exp_chunk $i $j $k
try find . -name '*.txt' -print > /tmp/test.manifest
try tar jcvf incomplete_edges_"$i"_"$j"_"$k".tar.bz2 --files-from /tmp/test.manifest
try aws s3 cp incomplete_edges_"$i"_"$j"_"$k".tar.bz2 s3://seunglab/region_graph_exp_data/incomplete_edges/
try aws s3 cp rg_volume_"$i"_"$j"_"$k".in s3://seunglab/region_graph_exp_data/region_graphs/

try touch "$i"_"$j"_"$k".txt
try aws s3 cp "$i"_"$j"_"$k".txt s3://seunglab/region_graph_data/done/

rm *.tar.bz2
find . -name '*.txt' | xargs rm -v

rm *.h5
