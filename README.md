# galera_daemon
This is a galera daemon, which helps with the scenario as belows:
```
node1 (X with power off.) node2 (O) (primary) node3 (O) (primary)
node1 (X with kill -9...) node2 (O) (primary) node3 (O) (primary)
node1 (X with kill -stop) node2 (O) (primary) node3 (O) (primary)
node1 (X with usual stop) node2 (O) (primary) node3 (O) (primary)
node1 (X ...............) node2 (X) (primary) node3 (O) (primary)
node1 (X ...............) node2 (X) (primary) node3 (X) (primary)    ==> no action, need manual fix  
```
