#!/bin/bash

for i in hadoop@hadoop200 hadoop@hadoop201 hadoop@hadoop202

do
    	echo “============== $i =================”
	ssh $i 'jps'
done

