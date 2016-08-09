# tools-elasticsearch
Elastic Search tools for checking, managing, observing cluster conditions.

## bash_aliases
Shortened set of basic frequently used informational commands run against ES to determine state.

- esBS: List Bad (UNASSIGNED) shards.
- esCSet: Cluster settings.
- esHealth: Performs a /_cat/health API call every 30 seconds.
- edNodes: List nodes in cluster ordered by heap size with load, heap. node type. and master status.
- esRelo: List relocating shards.
- esSCount: Count total shards per node.
- esTasks: Current count of pending tasks.

## es_report.py
Generates a report suitable for emailing that summarizes the ES cluster state.  Outputs cluster state size, index count, shard count, document count, data under management (gb), along with per-customer index grouping stats (days of retention, index #, shard #, document #, size, date range of indexes).

## move-all-shards.sh
Script to move all shards from a node and attempt to evenly distribute them across all other nodes.  Useful for excluding nodes when the shard count on a node would overwhelm available memory causing the node to OOM.  Suddently excluding a node can cause the node to OOM when attempting to move too many shards and that hoses the cluster.  You do have to stop shard rebalancing while running this, otherwise ES will try and put shards back on the soon-to-be excluded node.

## tellMeWhatToMove.py
Attempt to determine most active shards on busiest nodes and suggest move commands to balance a cluster based on indexing load versus shard count and disk utilization.