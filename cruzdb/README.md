# Development Notes

```bash
mvn install:install-file -Dfile=cruzdb-0.0.1.jar -DgroupId=com.cruzdb \
  -DartifactId=cruzdb -Dversion=0.0.1 -Dpackaging=jar

export LD_LIBRARY_PATH=$HOME/src/zlog/src/java/native

mvn -pl com.yahoo.ycsb:cruzdb-binding -am clean package

export LD_LIBRARY_PATH=$HOME/src/zlog/src/java/native

bin/ycsb load cruzdb -threads 2 -s -P workloads/workloada \
  -p recordcount=10000 -p cruzdb.lmdb.dir=$PWD/db2 

ZLOG_LMDB_BE_SIZE=GBS
```
