/**
 * Copyright (c) 2016 YCSB contributors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db.cruzdb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.*;
import static com.yahoo.ycsb.db.cruzdb.CruzUtils.serializeTable;
import static com.yahoo.ycsb.db.cruzdb.CruzUtils.createResultHashMap;
import com.cruzdb.Log;
import com.cruzdb.CruzIterator;
import com.cruzdb.Transaction;

/**
 * CruzDB client for the YCSB framework.
 */
public class CruzDBClient extends DB {

  private static Log log;
  private static com.cruzdb.DB db;
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    synchronized (CruzDBClient.class) {
      Properties props = new Properties();

      if (db != null) {
        System.out.println("Reusing existing CruzDB client");
        return;
      }

      // load default properties
      try {
        InputStream propFile = CruzDBClient.class.
            getClassLoader().getResourceAsStream("cruzdb.properties");
        Properties defaultProps = new Properties();
        defaultProps.load(propFile);
        props.putAll(defaultProps);
      } catch (Exception e) {
        System.err.println("Could not load properties file: " + e.toString());
        e.printStackTrace();
      }

      // fold in properties from command line
      props.putAll(getProperties());

      // open/create the log and database
      try {
        String logName = props.getProperty("cruzdb.logName");
        System.out.println("Opening (or creating) log: " + logName);
        String lmdbDir = props.getProperty("cruzdb.lmdb.dir", "");
        if (lmdbDir.length() == 0) {
          String cephPool = props.getProperty("cruzdb.cephPool");
          String seqHost = props.getProperty("cruzdb.seqHost");
          int seqPort = Integer.parseInt(props.getProperty("cruzdb.seqPort"));
          System.out.println("Using Ceph pool: " + cephPool);
          System.out.println("Using sequencer: " + seqHost + ":" + seqPort);
          log = Log.openCeph(cephPool, seqHost, seqPort, logName);
          db = com.cruzdb.DB.open(log, true);
        } else {
          System.out.println("Using LMDB directory: " + lmdbDir);
          log = Log.openLMDB(lmdbDir, logName);
          db = com.cruzdb.DB.open(log, true);
        }
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      if (db != null) {
        db.dispose();
        log.dispose();
        db = null;
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    final String compositeKey = table + "_" + key;
    try {
      final byte[] values = db.get(compositeKey.getBytes());
      if (values == null) {
        return Status.NOT_FOUND;
      }
      createResultHashMap(fields, values, result);
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    final String compositeKey = table + "_" + startkey;
    final CruzIterator iterator = db.newIterator();
    try {
      int count = 0;
      iterator.seek(compositeKey.getBytes());
      while (iterator.isValid() && count < recordcount) {
        final HashMap<String, ByteIterator> values = new HashMap<>();
        createResultHashMap(fields, iterator.value(), values);
        result.add(values);
        iterator.next();
        count++;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      iterator.dispose();
    }
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    final String compositeKey = table + "_" + key;
    final byte[] valueBlob = serializeTable(values);
    try {
      db.put(compositeKey.getBytes(), valueBlob);
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    final String compositeKey = table + "_" + key;
    final Transaction txn = db.newTransaction();
    try {
      final byte[] keyBytes = compositeKey.getBytes();
      final byte[] curValues = txn.get(keyBytes);
      if (curValues == null) {
        try {
          txn.abort();
        } catch (Exception e) {
          System.err.println(e.toString());
        }
        return Status.NOT_FOUND;
      }
      final HashMap<String, ByteIterator> result = new HashMap<>();
      createResultHashMap(null, curValues, result);
      result.putAll(values);
      txn.put(keyBytes, serializeTable(result));
      txn.commit();
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      txn.dispose();
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    final String compositeKey = table + "_" + key;
    try {
      db.delete(compositeKey.getBytes());
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
    return Status.OK;
  }
}
