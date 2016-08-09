#!/usr/bin/env python

## tellMeWhatToMove.py - Calculate most active shards/nodes and determine
##   how to rebalance the cluster by moving shards.
##
##   Should have option to limit by size.
##   Should perform a shard swap so that shard counts across nodes
##   remain the same.
"""
usage: tellMeWhatToMove.py [-h] [-c CLUSTER] [-H HOST] [-P PORT] [-t TEMP]
                           [-g] [-l LIMIT] [-v] [-m {load,dedup}]

Generate a customer index report against ElasticSearch.

optional arguments:
  -h, --help            show this help message and exit
  -c CLUSTER, --cluster CLUSTER
                        Which ES Cluster this report is for.
  -H HOST, --host HOST  An ES host in to query.
  -P PORT, --port PORT  ES HTTP API Port (default: 9200)
  -t TEMP, --temp TEMP  Temp directory to write files.
  -g, --nogather        Do not collect new shard data. Use existing files.
  -l LIMIT, --limit LIMIT
                        Max number of moves to suggest. (default 5. 0 means no
                        moves)
  -v, --verbose         Increase verbosity.
  -m {load,dedup}, --method {load,dedup}
                        Select move determination method.

Examples:
tellMeWhatToMove.py
  all defaults - collect activity data, determine the top 5 shards to move from
  the busiest host and show move commands.

tellMeWhatToMove.py -g
  same as above, but do not collect new activity data.  

tellMeWhatToMove.py -g -l 0
  only collect node load and current list of shards.  Do no move calculations.
  display node utilization information like:
2016-05-15 01:20:02,794 [INFO] Skipping shard activity data collection.
2016-05-15 01:20:02,802 [INFO] Starting new HTTP connection (1): localhost.localdomain
2016-05-15 01:20:12,488 [INFO] Retrieving data node load average
2016-05-15 01:20:12,490 [INFO] Starting new HTTP connection (1): localhost.localdomain
2016-05-15 01:20:22,845 [INFO] Node            Load       Docs [shards]       (space): Percent [shards] ( space)
2016-05-15 01:20:22,845 [INFO] es4c   0.65  196561951 [  4123]  ( 581.23 gb):   3.81% [11.10%] ( 4.05%)
2016-05-15 01:20:22,845 [INFO] es4b   1.45  644560320 [  4131]  (1793.21 gb):  12.49% [11.12%] (12.51%)
2016-05-15 01:20:22,845 [INFO] es4a   0.75  642266619 [  4131]  (1789.74 gb):  12.44% [11.12%] (12.48%)
2016-05-15 01:20:22,846 [INFO] es2b   0.90  564478620 [  4127]  (1584.53 gb):  10.93% [11.11%] (11.05%)
2016-05-15 01:20:22,846 [INFO] es3b   1.74  658679305 [  4132]  (1823.19 gb):  12.76% [11.12%] (12.72%)
2016-05-15 01:20:22,846 [INFO] es2a   2.67  596213877 [  4126]  (1637.88 gb):  11.55% [11.11%] (11.43%)
2016-05-15 01:20:22,846 [INFO] es3c   1.06  644128301 [  4133]  (1790.28 gb):  12.48% [11.13%] (12.49%)
2016-05-15 01:20:22,846 [INFO] es2c   1.30  633346148 [  4126]  (1731.03 gb):  12.27% [11.11%] (12.08%)
2016-05-15 01:20:22,846 [INFO] es3a   2.44  581990865 [  4120]  (1604.29 gb):  11.27% [11.09%] (11.19%)
2016-05-15 01:20:22,846 [INFO] ##### Totals:        5162226006 [ 37149] (14335.38 gb) 
"""

import re
import os
import sys
import time
import requests
import argparse
import logging
from collections import namedtuple
from operator import attrgetter, itemgetter, methodcaller
from datetime import datetime


def getShardDataDisk (esMaster, esPort, firstPass, fileName):
  """
  Gathers shard data from ES.  If firstPass is a dict-array, then this
  is the 2nd pass to determine doc-writes/minute for each shard.  Otherwise
  shard activity is 0

  returns total number of documents in ES
  """
  totalDocs = 0
  with open(fileName, 'w') as handle:
      req = requests.get('http://%s:%s/_cat/shards?bytes=b' % (esMaster, esPort), stream=True)
      for esShards in req.iter_lines():
          if re.search('STARTED', esShards):
              fields = esShards.split()
              totalDocs += int(fields[4])
              shardId = fields[0] + "." + fields[1] + "." + fields[7]
              ## File Format is:
              ## node index shard# shard-activity totalDocsInShard sizeOfShardBytes
              if type(firstPass) is int:
                handle.write("%s %s %s %d %d %d\n" % 
                             (fields[7], fields[0], fields[1], 0, int(fields[4]), int(fields[5])))
              elif shardId in firstPass:
                handle.write("%s %s %s %d %d %d\n" % 
                             (fields[7], fields[0], fields[1], 
                              int(fields[4]) - int(firstPass[shardId][0]), int(fields[4]), int(fields[5])))

  handle.close()
  return totalDocs


def getShardDataMem (esMaster, esPort):
  """
  Query ES for shard data on all ACTIVE shards.
  Store and return dict of results.

  returns shards dict
  """
  shards = dict()
  req = requests.get('http://%s:%s/_cat/shards?bytes=b' % (esMaster, esPort), stream=True)
  for esShards in req.iter_lines():
      if re.search('STARTED', esShards):
          fields = esShards.split()
          shardId = fields[0] + "." + fields[1] + "." + fields[7]
          shards[shardId] = (fields[4], fields[5])
          
  return shards


def totalDocsFromDisk (fileName):
  """
  Get count of total docs from existing shard data file.
  """
  totalDocs = 0
  with open(fileName, 'r') as sHandle:
    for shards in sHandle.readlines():
      fields = shards.split()
      totalDocs += int(fields[4])
  sHandle.close()
  return totalDocs

def collectShardActivity (esMaster, esPort, tmpDir):
  """
  Performs a 2 pass request against ES for shard data.
  The second pass is 1 minute after the first in order to
  determine doc-writes/minute per shard.

  returns total # of docs in ES
  """
  totalDocs = 0

  logging.info("Collecting first set of shard data into memory.")
  shards = getShardDataMem(esMaster, esPort)
  ## Waiting a minute to be able to get docs/minute count.
  logging.info("Sleeping for a minute... try and relax.")  
  time.sleep(60)

  logging.info("Collecting second set of shard data into %s." % tmpDir + ".1")

  ## Write second pass
  return getShardDataDisk(esMaster, esPort, shards, tmpDir + ".1")


def collectNodeData (esMaster, esPort, tmpDir):
  """
  Get the load for each datanode in the ES cluster.
  returns nothing
  """
  nodeF1 = tmpDir + ".5"
  nodeF2 = tmpDir + ".6"

  logging.info("Retrieving data node load average")
  with open(nodeF1, 'w') as handle1:
      with open(nodeF2, 'w') as handle2:
          req = requests.get('http://%s:%s/_cat/nodes?h=name,load,node,node.role' % 
                             (esMaster, esPort), stream=True)
          for esNodes in req.iter_lines():
              fields = esNodes.split()
              if (len(fields) >= 3) and (fields[2] == 'd'):
                  zone = fields[0].split('-')
                  handle1.write("ZONE %s %s\n" % (fields[0], zone[2][-1:]))
                  handle2.write("LOADAVE %s %s\n" % (fields[0], fields[1]))
  handle1.close()
  handle2.close()

def calcShardActivity(tmpDir, totDocs = 1):
  """
  Show based on system load, the amount of docs and space
  that a node occupies within the ES cluster.

  nodeUsage - dict of nodes w/ array of counts [docs, disk bytes, shards]
  nodeShards - dict of nodes w/ array of shard information
  returns nodeUsage and nodeShards 
  """
  shD1 = tmpDir + ".1"
  
  nLoad = tmpDir + ".6"
  nZone = tmpDir + ".5"

  nodeUsage = {}
  nodeShards = {}

  with open(nLoad, 'r') as nHandle:
    for nodes in nHandle.readlines():
      fields = nodes.split()
      ## Fields: documents, disk, shards
      nodeUsage[fields[1]] = [ float(fields[2]), 0, 0, 0 ]
  nHandle.close()

  totDocs = 0
  totDisk = 0
  totShards = 0
  with open(shD1, 'r') as sHandle:
    for shards in sHandle.readlines():
      fields = shards.split()
      ## Add to document count for node
      nodeUsage[fields[0]][1] += int(fields[4])
      totDocs += int(fields[4])
      ## diskspace used
      nodeUsage[fields[0]][2] += int(fields[5])
      totDisk += int(fields[5])
      ## shard count
      nodeUsage[fields[0]][3] += 1
      totShards += 1

      if fields[0] in nodeShards:
          nodeShards[fields[0]].append([fields[1], fields[2], fields[3], fields[5]])
      else:
          nodeShards[fields[0]] = [[fields[1], fields[2], fields[3], fields[5]]]
  sHandle.close()
  
  logging.info("Node            Load       Docs [shards]       (space): Percent [shards] ( space)")
  for node in nodeUsage.iterkeys():
    logging.info("%s %6.2f %10d [%6d]  (%7.2f gb):  %5.2f%% [%5.2f%%] (%5.2f%%)" % (node, float(nodeUsage[node][0]),  ## node, load
                                                                                  int(nodeUsage[node][1]),          ## docs
                                                                                  int(nodeUsage[node][3]),          ## shards
                                                                                  float(nodeUsage[node][2]) / 1073741824.00, ## space
                                                                                  float(nodeUsage[node][1] * 100) / float(totDocs), ## %docs
                                                                                  float(nodeUsage[node][3] * 100) / float(totShards), ## %shards
                                                                                  float(nodeUsage[node][2] * 100) / float(totDisk))) ## %disk

                                                                                  
  logging.info("##### Totals:       %11d [%6d] (%2.2f gb)" % (int(totDocs), int(totShards), float(totDisk) / 1073741824.00))
  return nodeUsage, nodeShards

def shardsToMoveLoad(usage, shards, limit = 10):
  """
  Figure out which shards to move from what node to another node.
  Source node is the one with the highest load.
  Destination node is the one with the lowest load.
  Shard to move is based on change in # of docs over one minute.
  Will not move a shard to a node that already has a shard of that index.
  """
  hotNode = ''
  curLoad = 0.0
  for node in usage:
      if usage[node][0] > curLoad:
          hotNode = node
          curLoad = usage[node][0]
  ## Find 'limit' # of shards that should be moved.
  curCount = 0
  usageHiLo = sorted(usage.items(), key=itemgetter(1), reverse=True)
  usageLoHi = sorted(usage.items(), key=itemgetter(1))

  for (node, values) in usageHiLo:
    if curCount >= limit:
      break

    for a in sorted(shards[node], key=itemgetter(2), reverse=True):
      if curCount >= limit:
        break
      curCount = curCount + 1

      sNode = findIdx(a[0], usageLoHi, shards, 1)
      if sNode:
        logging.info("Move index %s, shard %s from %s to %s" % (a[0], a[1], node, sNode))
        writeShardMove(a[0], a[1], node, sNode, esHost, esPort)
      else:
        logging.warn("Could not find node to move %s from %s" % (a[0], node))


def findIdx (index, nodes, shards, limit = 1):
  """
  Find a node where the number of shards for an index is
  below the limit of shards allowed per node.

  returns the first node that qualifies or False if no nodes can be found
  """
  for (sNode, sValues) in nodes:
    onHost = 0
    if sNode != "es2a":
      for sIdx in shards[sNode]:
        if index in sIdx:
          onHost += 1
          if onHost >= limit:
            break
          
      if onHost < limit:
        return sNode
      
  return False

def shardsToMoveDeDup(usage, shards, limit = 1):
  """
  Get rid of duplicate indexes on hosts.
  limit is the max number of shards from an index that can be on a node.
  """
  usageHiLo = sorted(usage.items(), key=itemgetter(1), reverse=True)
  usageLoHi = sorted(usage.items(), key=itemgetter(1))
  
  for (node, loads) in usageHiLo:
    ## Look for duplication of indexes
    curCount = 1
    curIdx = ""
    for a in sorted(shards[node], key=itemgetter(0)):
      if a[0] == curIdx:
        curCount += 1
      else:
        if curCount > limit:
          logging.warn("[%s] %d shards of index %s" % (node, curCount, curIdx))
        curIdx = a[0]
        curCount = 1

## This would suggest the actual moves per shard...  
## If this is really bad, then probably better to adjust ES settings
## than do this manually.        
#        sNode = findIdx(a[0], usageLoHi, shards, 2)
#        if sNode:
#          logging.info("Move index %s, shard %s from %s to %s" % (a[0], a[1], node, sNode))
#          if sNode in shards:
#            shards[sNode].append([a[0], a[1], 0, 0])
#          else:
#            shards[sNode] = [[a[0], a[1], 0, 0]]
#          
#          writeShardMove(a[0], a[1], node, sNode, esHost, esPort)
#        else:
#          logging.warn("Could not find node to move %s from %s" % (a[0], node))


def writeShardMove(index, shard, source, destination, master = "", port="9200"):
  if not master:
    master = destination

  logging.info("Move Command:\n\tdate; curl -XPOST '%s:%s/_cluster/reroute' -d '{ \"commands\": [{ \"move\": { \"index\": \"%s\", \"shard\": \"%s\", \"from_node\": \"%s\", \"to_node\": \"%s\" }}]}' | cut -c1-160" % (master, port, index, shard, source, destination))

###############################################
## Argument parsing...
parser = argparse.ArgumentParser(description='Generate a customer index report against ElasticSearch.')
parser.add_argument("-c", "--cluster", default="Production", 
                    help="Which ES Cluster this report is for.")
parser.add_argument("-H", "--host", default="localhost", 
                    help="An ES host in to query.")
parser.add_argument("-P", "--port", default="9200", type=int,
                    help="ES HTTP API Port (default: 9200)")                    
parser.add_argument("-t", "--temp", default="/tmp", 
                    help="Temp directory to write files.")
parser.add_argument("-g", "--nogather", default=False, action='store_true', 
                    help="Do not collect new shard data.  Use existing files.")
parser.add_argument("-l", "--limit", default=5, type=int,
                    help="Max number of moves to suggest. (default 5. 0 means no moves)")
parser.add_argument("-v", "--verbose", action='store_true', 
                    help="Increase verbosity.")
parser.add_argument("-m", "--method", default="load", choices=['load','dedup'],
                    help="Select move determination method.")

args = parser.parse_args()

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                   format='%(asctime)s [%(levelname)s] %(message)s'
                   )

if args.verbose:
    logging.basicConfig(level=logging.INFO)

esHost = args.host
esPort = args.port
tempDir = args.temp
clusterName = args.cluster

fnamePre = tempDir + "/tmwtm-" + str(os.geteuid())

if args.nogather and args.limit > 0:
  logging.info("Using exiting data collection files.")
  totShards = totalDocsFromDisk(fnamePre + ".1")
elif args.nogather and args.limit == 0:
  logging.info("Skipping shard activity data collection.")
  totShards = getShardDataDisk(esHost, esPort, 0, fnamePre + ".1")
  collectNodeData(esHost, esPort, fnamePre)  
else:
  totShards = collectShardActivity(esHost, esPort, fnamePre)
  collectNodeData(esHost, esPort, fnamePre)

(usage, shards) = calcShardActivity(fnamePre, totShards)

if args.limit > 0:
  if args.method == "dedup":
    shardsToMoveDeDup(usage, shards, args.limit)
  else:
    logging.info("Determining which shards to move.")
    shardsToMoveLoad(usage, shards, args.limit)

  
