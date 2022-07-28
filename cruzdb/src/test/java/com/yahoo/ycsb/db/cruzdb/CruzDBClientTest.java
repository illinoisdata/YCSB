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

import java.util.*;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Status;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;

public class CruzDBClientTest {
  private static final String table = "table";
  private static final String keyPrefix = "key-";
  private static final String fieldPrefix = "field-";
  private static final int numRows = 100;
  private static final int numFields = 10;

  private CruzDBClient client;

  private static String rowKey(int rowId) {
    return keyPrefix + String.format("%06d", rowId);
  }

  private static String fieldKey(int fieldId) {
    return fieldPrefix + fieldId;
  }

  private static String fieldValue(int rowId, int fieldId) {
    return Integer.toString(rowId * numFields + fieldId);
  }

  private static HashMap<String, String> createRow(int rowId,
      Set<Integer> fieldIds) {
    HashMap<String, String> result = new HashMap<>();
    for (Integer fieldId : fieldIds) {
      final int i = fieldId.intValue();
      result.put(fieldKey(i), fieldValue(rowId, i));
    }
    return result;
  }

  private static HashMap<String, String> createRow(int rowId) {
    Set<Integer> fieldIds = new HashSet<Integer>();
    for (int i = 0; i < numFields; i++) {
      fieldIds.add(new Integer(i));
    }
    return createRow(rowId, fieldIds);
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();

    final String dbPath = tempFolder.getRoot().getPath();
    props.setProperty("cruzdb.lmdb.dir", dbPath);

    // this isn't important when using the lmdb backend because we run the
    // tests in a fresh backend instance. with a ceph backend we don't
    // currently have a good way to delete logs, so each test is run with a
    // fresh log created by using a unique name.
    final String logName = "cruzdb.ycsb.test." + System.currentTimeMillis();
    props.setProperty("cruzdb.logName", logName);

   //	 // temp client used to pre-populate db
   //	 CruzDBClient c = new CruzDBClient();
   //	 c.setProperties(props);
   //	 c.init();

   //	 for (int rowId = 0; rowId < numRows; rowId++) {
   //	   final String key = rowKey(rowId);
   //	   final Map<String, ByteIterator> row =
   //	     StringByteIterator.getByteIteratorMap(createRow(rowId));
   //	   c.insert(table, key, row);
   //	 }

   //	 // make sure a new low-level db connection is re-built
   //	 c.cleanup();

    // client instance used by the test
    client = new CruzDBClient();
    client.setProperties(props);
    client.init();
    for (int rowId = 0; rowId < numRows; rowId++) {
      final String key = rowKey(rowId);
      final Map<String, ByteIterator> row =
        StringByteIterator.getByteIteratorMap(createRow(rowId));
      client.insert(table, key, row);
    }
  }

  @After
  public void tearDown() throws Exception {
    // force clean-up before the next test runs so that the low-level db
    // client will be recreated in the next test, otherwise the new instances
    // of the client will share the same low-level db connection.
    client.cleanup();
    client = null;
  }

  @Test
  public void testInsert() {
    final String key = rowKey(numRows);

    // row not found
    assertEquals(Status.NOT_FOUND,
        client.read(table, key, null, null));

    // insert row
    HashMap<String, String> row = createRow(numRows);
    assertEquals(Status.OK,
        client.insert(table, key,
          StringByteIterator.getByteIteratorMap(row)));

    // verify exists as expected
    HashMap<String, ByteIterator> result = new HashMap<>();
    assertEquals(Status.OK,
        client.read(table, key, null, result));
    assertEquals(row, StringByteIterator.getStringMap(result));
  }

  @Test
  public void testUpdateNotFound() {
    final String key = rowKey(numRows);
    assertEquals(Status.NOT_FOUND,
        client.update(table, key, null));
  }

  @Test
  public void testUpdate() {
    for (int rowId = 0; rowId < numRows; rowId++) {
      final String key = rowKey(rowId);

      // read previous row state
      HashMap<String, ByteIterator> prevRow = new HashMap<>();
      assertEquals(Status.OK,
          client.read(table, key, null, prevRow));

      // randomized list of all field ids
      ArrayList<Integer> allFieldIds = new ArrayList<Integer>();
      for (int i = 0; i < numFields; i++) {
        allFieldIds.add(new Integer(i));
      }
      Collections.shuffle(allFieldIds, new Random(0));

      // take subset of field ids for updating
      HashSet<Integer> fieldIds = new HashSet<Integer>();
      HashSet<String> fieldKeys = new HashSet<String>();
      final int fieldCount = new Random().nextInt(numFields);
      for (int i = 0; i < fieldCount; i++) {
        final int fieldId = allFieldIds.get(i).intValue();
        fieldIds.add(fieldId);
        fieldKeys.add(fieldKey(fieldId));
      }

      // build the update
      HashMap<String, String> rowUpdate = new HashMap<>();
      for (String fieldKey : fieldKeys) {
        rowUpdate.put(fieldKey, "UPDATED");
      }

      // perform partial row update
      assertEquals(Status.OK,
          client.update(table, key,
            StringByteIterator.getByteIteratorMap(rowUpdate)));

      // read up new row
      HashMap<String, ByteIterator> newRow = new HashMap<>();
      assertEquals(Status.OK,
          client.read(table, key, null, newRow));

      // build expected
      prevRow.putAll(StringByteIterator.getByteIteratorMap(rowUpdate));

      // test
      assertEquals(StringByteIterator.getStringMap(prevRow),
          StringByteIterator.getStringMap(newRow));
    }
  }

  @Test
  public void testScan() {
    // get all fields
    for (int startRowId = 0; startRowId < numRows; startRowId++) {
      Vector<HashMap<String, ByteIterator>> scannedValues = new Vector<>();

      final String startKey = rowKey(startRowId);
      assertEquals(Status.OK,
          client.scan(table, startKey, numRows, null, scannedValues));

      for (int rowId = startRowId; rowId < numRows; rowId++) {
        HashMap<String, String> expected = createRow(rowId);
        HashMap<String, ByteIterator> result =
          scannedValues.get(rowId - startRowId);
        assertEquals(expected, StringByteIterator.getStringMap(result));
      }
    }

    // get subset of fields
    for (int startRowId = 0; startRowId < numRows; startRowId++) {

      // randomized list of all field ids
      ArrayList<Integer> allFieldIds = new ArrayList<Integer>();
      for (int i = 0; i < numFields; i++) {
        allFieldIds.add(new Integer(i));
      }
      Collections.shuffle(allFieldIds, new Random(0));

      // take subset of field ids
      HashSet<Integer> fieldIds = new HashSet<Integer>();
      HashSet<String> fieldKeys = new HashSet<String>();
      final int fieldCount = new Random().nextInt(numFields);
      for (int i = 0; i < fieldCount; i++) {
        final int fieldId = allFieldIds.get(i).intValue();
        fieldIds.add(fieldId);
        fieldKeys.add(fieldKey(fieldId));
      }

      Vector<HashMap<String, ByteIterator>> scannedValues = new Vector<>();

      final String startKey = rowKey(startRowId);
      assertEquals(Status.OK,
          client.scan(table, startKey, numRows, fieldKeys, scannedValues));

      for (int rowId = startRowId; rowId < numRows; rowId++) {
        HashMap<String, String> expected = createRow(rowId, fieldIds);
        HashMap<String, ByteIterator> result =
          scannedValues.get(rowId - startRowId);
        assertEquals(expected, StringByteIterator.getStringMap(result));
      }
    }
  }

  @Test
  public void testReadNotFound() {
    final String key = rowKey(numRows);
    assertEquals(Status.NOT_FOUND,
        client.read(table, key, null, null));
  }

  @Test
  public void testRead() {
    // all the rowIds in a random order
    ArrayList<Integer> rowIds = new ArrayList<Integer>();
    for (int i = 0; i < numRows; i++) {
      rowIds.add(new Integer(i));
    }
    Collections.shuffle(rowIds, new Random(0));

    // retrieve all fields
    for (Integer rowId : rowIds) {
      final String key = rowKey(rowId.intValue());

      HashMap<String, ByteIterator> result = new HashMap<>();
      assertEquals(Status.OK,
          client.read(table, key, null, result));

      HashMap<String, String> expected = createRow(rowId.intValue());

      assertEquals(expected, StringByteIterator.getStringMap(result));
    }

    // retrieve field subset
    for (Integer rowId : rowIds) {
      // randomized list of all field ids
      ArrayList<Integer> allFieldIds = new ArrayList<Integer>();
      for (int i = 0; i < numFields; i++) {
        allFieldIds.add(new Integer(i));
      }
      Collections.shuffle(allFieldIds, new Random(0));

      // take subset of field ids
      HashSet<Integer> fieldIds = new HashSet<Integer>();
      HashSet<String> fieldKeys = new HashSet<String>();
      final int fieldCount = new Random().nextInt(numFields);
      for (int i = 0; i < fieldCount; i++) {
        final int fieldId = allFieldIds.get(i).intValue();
        fieldIds.add(fieldId);
        fieldKeys.add(fieldKey(fieldId));
      }

      final String key = rowKey(rowId.intValue());

      HashMap<String, ByteIterator> result = new HashMap<>();
      assertEquals(Status.OK,
          client.read(table, key, fieldKeys, result));

      HashMap<String, String> expected = createRow(rowId.intValue(), fieldIds);

      assertEquals(expected, StringByteIterator.getStringMap(result));
    }
  }

  @Test
  public void testDelete() {
    final String key = rowKey(new Random().nextInt(numRows));

    // row exists now
    HashMap<String, ByteIterator> result = new HashMap<>();
    assertEquals(Status.OK, client.read(table, key, null, result));

    // poof!
    assertEquals(Status.OK, client.delete(table, key));
    assertEquals(Status.NOT_FOUND, client.read(table, key, null, null));
  }
}
