# galera_daemon
This is a galera daemon, which helps with the scenario as belows:
1. node1 (X with power off.)    node2 (O) (primary)    node3 (O) (primary)
2. node1 (X with kill -9...)    node2 (O) (primary)    node3 (O) (primary)
3. node1 (X with kill -stop)    node2 (O) (primary)    node3 (O) (primary)
4. node1 (X with usual stop)    node2 (O) (primary)    node3 (O) (primary)
5. node1 (X ...............)    node2 (X) (primary)    node3 (O) (primary)
6. node1 (X ...............)    node2 (X) (primary)    node3 (X) (primary)    ==> keep no-action
