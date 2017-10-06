# Development Notes

Build the zlog/cruzdb java bindings and install them into the local maven
repository. Below we rename the bindings from zlog.jar to cruzdb-version.jar. We
currently lack a workflow for building these bindings with consistent names and
versions.

```bash
[nwatkins@sapporo java]$ mv zlog.jar cruzdb-0.0.1.jar
[nwatkins@sapporo java]$ mvn install:install-file -Dfile=cruzdb-0.0.1.jar -DgroupId=com.cruzdb -DartifactId=cruzdb -Dversion=0.0.1 -Dpackaging=jar
```

Setup environment.

```bash
export LD_LIBRARY_PATH=$HOME/src/zlog/build/src/java/native
ZLOG_LMDB_BE_SIZE=GBS
```

Build and then run a workload.

```bash
mvn -pl com.yahoo.ycsb:cruzdb-binding -am clean package

bin/ycsb load cruzdb -threads 2 -s -P workloads/workloada \
  -p recordcount=10000 -p cruzdb.lmdb.dir=$PWD/db2 
```
