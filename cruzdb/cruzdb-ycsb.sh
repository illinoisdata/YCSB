CRUZDB_DIR=$PWD/cruzdb
YCSB_DIR=$PWD/YCSB
ZLOG_DIR=$PWD/zlog

ZLOG_JAVA_BINDINGS=${ZLOG_DIR}/src/java
CRUZDB_JAVA_BINDINGS=${CRUZDB_DIR}/src/java
YCSB_CRUZDB=${YCSB_DIR}/cruzdb


install() {
        sudo apt install liblmdb-dev
        sudo apt install libradospp-dev
        git config --global url."https://github".insteadOf git://github # Otherwise recursive clone will result in an error
        git clone --recursive https://github.com/illinoisdata/zlog.git
        git clone --recursive https://github.com/illinoisdata/cruzdb.git
        git clone -b cruzdb https://github.com/illinoisdata/YCSB.git
}

zlog() {
        cd ${ZLOG_DIR}
        ./install-deps.sh
        cmake .
        make
        make install

        cd ${ZLOG_JAVA_BINDINGS}
        cmake .
        make
        make install # Make sure libzlogjni.so installed in /usr/local/lib
        mv zlog.jar zlog-0.0.1.jar
        cp -R zlog-0.0.1.jar ${YCSB_CRUZDB}
        echo "------------------------------"
        echo "ZLOG JAVA BINDINGS INSTALLED"
        echo "------------------------------"
}

cruzdb() {
        cd ${CRUZDB_DIR}
        ./install-deps.sh
        cmake -DZLOG_INSTALL_DIR=/../ .
        make
        make install

        cd ${CRUZDB_JAVA_BINDINGS}
        mkdir zlog-jar
        cp -R ${ZLOG_JAVA_BINDINGS}/zlog-0.0.1.jar ./zlog-jar
        cp -R ${CRUZDB_DIR}/src/include/cruzdb/options.h /usr/local/include/cruzdb/
        cmake -DZLOG_JAVA=zlog-jar/zlog-0.0.1.jar .
        make
        make install # Make sure libcruzdbjni.so installed in /usr/local/lib
        mv cruzdb.jar cruzdb-0.0.1.jar
        cp -R cruzdb-0.0.1.jar ${YCSB_CRUZDB}
        echo "------------------------------"
        echo "CRUZDB JAVA BINDINGS INSTALLED"
        echo "------------------------------"
}

ycsb() {
        # Should have zlog-0.0.1.jar and cruzdb-0.0.1.jar in YCSB/cruzdb folder before calling this function
        cd ${YCSB_CRUZDB}
        mvn install:install-file -Dfile=cruzdb-0.0.1.jar -DgroupId=com.cruzdb -DartifactId=cruzdb -Dversion=0.0.1 -Dpackaging=jar
        mvn install:install-file -Dfile=zlog-0.0.1.jar -DgroupId=com.cruzdb -DartifactId=zlog -Dversion=0.0.1 -Dpackaging=jar
        cd ${YCSB_DIR}
        export LD_LIBRARY_PATH=/usr/local/lib
        ZLOG_LMDB_BE_SIZE=GBS
        mvn -pl com.yahoo.ycsb:cruzdb-binding -am clean package
}

install
zlog
cruzdb
ycsb