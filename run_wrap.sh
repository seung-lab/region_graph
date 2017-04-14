#!/bin/bash
apt-get install parallel
while [ ! -f /tmp/finished.txt ]
do
    /mnt/data01/run_1.sh $*
done
rm /tmp/finished.txt
