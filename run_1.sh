#!/bin/bash
yell() { echo "$0: $*" >&2;  }
die() { yell "$*"; exit 111;  }
bail() { yell "$*"; exit 0; }
try() { "$@" || die "cannot $*";  }
try_bail() { "$@" && touch /tmp/finished.txt && bail "skip $*"; }

i=$1
j=$2
k=$3

export PATH=/root/julia-6445c82d00/bin:$PATH
export HOME=/root

cd /mnt/data01

try_bail aws s3 cp s3://seunglab/ca_data2/done/"$i"_"$j"_"$k".txt .

try timeout 60m julia cut_segments.jl aff $i $j $k
try timeout 30m julia cut_segments.jl seg $i $j $k

try julia ca_mmap.jl aff_chunk seg_exp_chunk $i $j $k
try julia split_complete_task.jl aff_chunk $i $j $k
#julia combine_edges_ca.jl
#try seq 0 7|parallel -j8 julia combine_edges_ca.jl aff_chunk input_{}.txt output_{}.txt
#for x in {0..7}; do echo ${x}; try cat output_${x}.txt >> ca_volume_"$i"_"$j"_"$k".in; done
try find seg_exp_chunk -name '*.txt' -print > /tmp/test.manifest
try tar --use-compress-prog=pbzip2 -cf incomplete_edges_"$i"_"$j"_"$k".tar.bz2 --files-from /tmp/test.manifest
try aws s3 cp incomplete_edges_"$i"_"$j"_"$k".tar.bz2 s3://seunglab/ca_data2/incomplete_edges/ --quiet
try find aff_chunk -name '*.txt' -print > /tmp/test.manifest
try tar --use-compress-prog=pbzip2 -cf complete_edges_"$i"_"$j"_"$k".tar.bz2 --files-from /tmp/test.manifest
try aws s3 cp complete_edges_"$i"_"$j"_"$k".tar.bz2 s3://seunglab/ca_data2/complete_edges/ --quiet
#try bzip2 ca_volume_"$i"_"$j"_"$k".in
#try aws s3 cp ca_volume_"$i"_"$j"_"$k".in.bz2 s3://seunglab/ca_data2/ca_graphs/ --quiet

try touch "$i"_"$j"_"$k".txt
try aws s3 cp "$i"_"$j"_"$k".txt s3://seunglab/ca_data2/done/

find . -name '*.txt' | xargs rm
rm *.tar.bz2

rm *.h5
