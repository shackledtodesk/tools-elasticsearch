#!/usr/bin/env python

## es_report.py - generate a report on the state of an ES cluster
##
## TODO:
##   Send reports data to some metrics service.
##
## Information we are attempting to extract:
##   Cluster state size 
##   data under management (total mb/gb of indexes)
##   number of indexes total
##   number of shards total
##   number of total documents
##   space used per customer
##   number of indexes per customer
##   number of shards per customer
##   number of documents per customer

import re
import urllib2
import requests
import argparse
import logging
from collections import namedtuple
from operator import attrgetter
from datetime import datetime

## Default values
defESNode="localhost"
defClusterDescriptor="Production"

## Timeout in seconds
esTimeout = 90

orphanCount = 0
orphanIdx = []

esIdx = []
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger()

## Argument parsing...
parser = argparse.ArgumentParser(description='Generate a customer index report against ElasticSearch.')
parser.add_argument("-c", "--cluster", default="%s", help="Which ES Cluster this report is for." defClusterDescriptor)
parser.add_argument("-H", "--host", default="%s", help="An ES host in to query." % defESNode)
parser.add_argument("-P", "--port", default="9200", help="ES HTTP API Port (default: 9200)", type=int)
parser.add_argument("-v", "--verbose", action='store_true', help="Increase verbosity.")

args = parser.parse_args()
if args.verbose:
    logger.setLevel(logging.INFO)

esHost = args.host
esPort = args.port
clusterName = args.cluster


print "\nOverall %s Elastic Search Information" % clusterName
#########################################
## Get cluster state size... Actually do a try instead of assuming it works.
try:
  req = urllib2.Request("http://%s:%s/_cluster/state" % (esHost, esPort))
  reqData = urllib2.urlopen(url=req, timeout=esTimeout)
except urllib2.URLError, e:
  logging.error("URL failed: %s" % e.reason)
  exit()
except urllib2.HTTPError, e:
  logging.error("Server Error: %s" % e.code)
  exit(-1)
else:
  clusterSize = float(len(reqData.read())) / (1024.00 * 1024.00)
  print "Cluster State Size: %8.2f mb" % clusterSize


#########################################
## Get Index Part of the report
totIdx = 0
totShards = 0
totDocs = 0
totSize = 0

idxInfo = namedtuple("idxInfo", 'name shards documents size customer date')
req = requests.get("http://%s:%s/_cat/indices?bytes=b" % (esHost, esPort), stream=True, timeout=esTimeout)

for esIndex in req.iter_lines():
  fields = esIndex.split()

  if len(fields) >= 7:
    regRes = re.match(r'(.*)-(\d{4}\.\d{2}\.\d{2})', fields[2])
    if regRes:
      customer = regRes.group(1)
      idxDate = regRes.group(2)
    else:
      customer = fields[2]
      idxDate = "1970.01.01"

    node = idxInfo(fields[2], fields[3], int(fields[5]) - int(fields[6]), fields[7], customer, idxDate)
    totIdx += 1
    totShards += int(node.shards)
    totDocs += int(node.documents)
    totSize += int(node.size)
    
    if idxDate == "1970.01.01":
      orphanCount += 1
      orphanIdx.append(node)
    else:
      esIdx.append(node)

print "Indexes: %d\nShards: %d\nDocuments: %d" % (totIdx, totShards, totDocs)
print "Data Under Management: {0:0d} gb".format(totSize / (1024 * 1024 * 1024))
print "Orphaned Indexes: %d     (should be zero)" % orphanCount

if orphanCount > 0:
  print """
  * Note: Orphan indexes are those that do not conform to the [customer environment]-[YYYY.mm.dd] naming convention. 
          See the list of ophaned indexes after the Customer Metrics list.
"""

print "\nCustomer Metrics"
print "                                       Customer Environment\tindexes\tshards\t    docs\t data size\tnewest - oldest = retention"
curCust = False
idxCount = 0
shardCount = 0
docCount = 0
size = 0
oldest = "2038.12.31"
newest = "1970.01.01"
for a in sorted(esIdx, key=attrgetter('customer')):
  if (a.customer != curCust) and (curCust):
      retension = datetime.strptime(newest, "%Y.%m.%d") - datetime.strptime(oldest, "%Y.%m.%d")
      print "%60s\t%d\t%d\t%8d\t%8.2f mb\t%s - %s = %d" % (curCust, idxCount, shardCount, docCount, float(size) / (1024.00 * 1024.00), newest, oldest, retension.days + 1)

      idxCount = 0
      docCount = 0
      shardCount = 0
      size = 0
      oldest = "2038.12.31"
      newest = "1970.01.01"

  curCust = a.customer
  idxCount += 1
  docCount += int(a.documents)
  shardCount += int(a.shards)
  size += int(a.size)
  cDate = datetime.strptime(a.date, "%Y.%m.%d")
  if cDate < datetime.strptime(oldest,"%Y.%m.%d"):
      oldest = a.date
  if cDate > datetime.strptime(newest, "%Y.%m.%d"):
      newest = a.date

if orphanCount > 0:
  print "\n\nOrphaned Indexes"
  print "%48s\t%s\t%s\t%s" % ("index", "shards", "docs", "size")
  for a in sorted(orphanIdx, key=attrgetter('customer')):
    print "%48s\t%s\t%s\t%0.2f mb" % (a.customer, a.shards, a.documents, float(a.size) / (1024.00 * 1024.00))
