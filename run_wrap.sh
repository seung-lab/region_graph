#!/bin/bash
#apt-get install parallel
#apt-get install pbzip2
mkdir /mnt/data01/aff_chunk
mkdir /mnt/data01/seg_exp_chunk

while [ ! -f /tmp/finished.txt ]
do
    aws s3 cp s3://seunglab/contact_area/ /mnt/data01 --recursive
    chmod +x /mnt/data01/*.sh
    /mnt/data01/run_1.sh $*
done

rm /tmp/finished.txt
