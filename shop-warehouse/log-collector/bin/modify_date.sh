#!/bin/bash

for i in hadoop101 hadoop102 hadoop103
do
        echo "========== $i =========="
        ssh -t $i "sudo date -s $1"
done
