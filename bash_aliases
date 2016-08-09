## Copy to .bash_aliases or source the file to add some shortcut commands.
##
export MASTER="[ES Node Name]"
## ES Node list with heap, load, and role ordered by heap usage
alias esNodes="curl -s -XGET ${MASTER}:9200/_cat/nodes?h=host,hp,load,node.role,master | sort -nrk2"

## watch/query the cluster health over time.
alias esHealth="while true; do curl http://${MASTER}:9200/_cat/health; sleep 30; done"

## What shards are relocating
alias esRelo="curl -s -XGET ${MASTER}:9200/_cat/shards | grep RELO"

## What shards are unassigned
alias esBS="curl -s -XGET ${MASTER}:9200/_cat/shards | grep UNASS"

## shard counts per node
alias esSCount="curl -s -XGET ${MASTER}:9200/_cat/shards | awk '{ print \$8 }' | sort |uniq -c"

## pending cluster tasks
alias esTasks="curl -s -XGET ${MASTER}:9200/_cluster/pending_tasks?pretty=1 | grep -c insert_order"

## transient cluster settings
alias esCSet="curl -s -XGET ${MASTER}:9200/_cluster/settings?pretty"
