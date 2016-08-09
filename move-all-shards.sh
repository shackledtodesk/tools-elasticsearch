#!/bin/bash

## Move all Shards
## _Slowly_ move all the shards from one node onto other nodes in the cluster
## in preparation for decommissioning a host.
##
## The "goTo" variable is a list of node names that are acceptable destinations
##

goTo="es2a es2b es2c es2a es2b es2c es2a es2b es2c es2a es2b es2c"
batchC=`echo ${goTo} | wc -w`

## Hostname of a node to query for ES API commands
master="localhost"
mPort=9200

## Thresholds
docThreshold=9999
docThreshold=50000
moveMax=64
moveMin=32

## Counters and placeholders
moveLine=""
let dCount=0
let mCount=1
let tMoves=0

## Host we are decommissioning
moveFrom="es2a"

if [ ! -z "${1}" ]; then
    moveFrom=${1}
fi

function waitForMoves {
    rMoves=`curl -s http://${master}:${mPort}/_cat/health | awk '{ print $9 }'`
    while [ ${rMoves} -gt ${moveMin} ]; do
	echo -n "."
	sleep 60
	rMoves=`curl -s http://${master}:${mPort}/_cat/health | awk '{ print $9 }'`
    done
}

## Startup...
echo "About to move all shards from ${moveFrom}.  Make sure you've stopped ES rebalancing."
echo -n "Are you ready to continue? [y/n] "
read answer
if [ "${answer}" != "y" ]; then
    echo "Bailing out."
    exit
fi

echo "Fetching current shard list for ${moveFrom}."
## Get all the shards on a node and move them...
IFS=$'\n'; for i in `curl -s -XGET ${master}:${mPort}/_cat/shards | grep ${moveFrom} | sort -nk5 | awk '{ print $1, $2, $5 }'`; do
    idx=`echo $i | awk '{ print $1 }'`
    shard=`echo $i | awk '{ print $2 }'`
    docs=`echo $i | awk '{ print $3 }'`
    
    ## maintain a document count of shards being moved
    let dCount=dCount+${docs}

    moveTo=`echo $goTo | cut -f${mCount} -d" "`
    let mCount=mCount+1
    let tMoves=tMoves+1
    
    ## What we are doing...
    now=`date`
    echo "${now}  ${moveFrom} ${idx} : ${shard} -> ${moveTo}"
    moveLine="${moveLine}, "'{ "move" : { "index" : "'${idx}'", "shard" : "'${shard}'", "from_node" : "'${moveFrom}'", "to_node" : "'${moveTo}'" }}'

    ## We send in batches of "batchC" (currently 12).
    if [ ${mCount} -gt ${batchC} ]; then
	moveLine=`echo ${moveLine} | cut -c2-`
	now=`date`
	echo ${now}  curl -s -XPOST "${master}:${mPort}/_cluster/reroute?timeout=5m&master_timeout=5m" -d '{ "commands": ['${moveLine}']}'
	curl -s -XPOST "${master}:${mPort}/_cluster/reroute?timeout=5m&master_timeout=5m" -d '{ "commands": ['${moveLine}']}' | cut -c1-60 &
	sleep 30
	let mCount=1
	moveLine=""

	## pause before next batch if we are moving
	## big shards (document count corresponds to shard size well enough)
	if [ ${dCount} -gt ${docThreshold} ]; then
	    now=`date`
	    echo -n ${now}  "##### Sleeping to let moves happen (docs: ${dCount}, moves: ${tMoves}) ...."
	    let tMoves=0

	    ## see how many shards are already moving.  If none, then don't sleep.
	    waitForMoves
	    let dCount=0
	    echo

	## if we've already sent requests to move >48 shards, pause.
	elif [ ${tMoves} -gt ${moveMax} ]; then
	    now=`date`
	    echo -n  "${now}  ##### sleeping to let moves happen (docs: ${dCount}, moves: ${tMoves}) ...."
	    let tMoves=0

	    ## make sure none too many shards are waiting to move.
	    waitForMoves
	    echo

	## otherwise, keep going...
	else
	    now=`date`
	    echo "${now}  ##### no sleep, very small indexes (docs: ${dCount}, moves: ${tMoves}) ...."
	fi
    fi
done

if [ ! -z "${moveLine}" ]; then
    moveLine=`echo ${moveLine} | cut -c2-`
    now=`date`
    echo ${now} Last one.
    echo ${now}  curl -s -XPOST "${master}:${mPort}/_cluster/reroute?timeout=5m&master_timeout=5m" -d '{ "commands": ['${moveLine}']}'
    curl -s -XPOST "${master}:${mPort}/_cluster/reroute?timeout=5m&master_timeout=5m" -d '{ "commands": ['${moveLine}']}' | cut -c1-60
fi
    
