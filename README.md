# monoceros
p2p metrics aggregation engine for c12s platform

sh run.sh node1 127.0.0.1:7001 127.0.0.1:8001 127.0.0.1:9001 x x x x
sh run.sh node2 127.0.0.1:7002 127.0.0.1:8002 127.0.0.1:9002 node1 127.0.0.1:7001 node1 127.0.0.1:8001
sh run.sh node3 127.0.0.1:7003 127.0.0.1:8003 127.0.0.1:9003 node1 127.0.0.1:7001 node1 127.0.0.1:8001