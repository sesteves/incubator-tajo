/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package org.apache.tajo.engine.query;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestResultSetImpl {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static StorageManager sm;
  private static TableMeta scoreMeta;

  @BeforeClass
  public static void setup() throws Exception {
    util = new TajoTestingCluster();
    util.startMiniCluster(3);
    conf = util.getConfiguration();
    sm = new StorageManager(conf);

    Schema scoreSchema = new Schema();
    scoreSchema.addColumn("deptname", Type.TEXT);
    scoreSchema.addColumn("score", Type.INT4);
    scoreMeta = CatalogUtil.newTableMeta(scoreSchema, StoreType.CSV);
    TableStat stat = new TableStat();

    Path p = sm.getTablePath("score");
    sm.getFileSystem().mkdirs(p);
    Appender appender = StorageManager.getAppender(conf, scoreMeta, new Path(p, "score"));
    appender.init();
    int deptSize = 100;
    int tupleNum = 10000;
    Tuple tuple;
    long written = 0;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      String key = "test" + (i % deptSize);
      tuple.put(0, DatumFactory.createText(key));
      tuple.put(1, DatumFactory.createInt4(i + 1));
      written += key.length() + Integer.SIZE;
      appender.addTuple(tuple);
    }
    appender.close();
    stat.setNumRows(tupleNum);
    stat.setNumBytes(written);
    stat.setAvgRows(tupleNum);
    stat.setNumBlocks(1000);
    stat.setNumPartitions(100);
    scoreMeta.setStat(stat);
    sm.writeTableMeta(sm.getTablePath("score"), scoreMeta);
  }

  @AfterClass
  public static void terminate() throws IOException {
    util.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException, SQLException {
    ResultSetImpl rs = new ResultSetImpl(null, null, conf, sm.getTablePath("score"));
    ResultSetMetaData meta = rs.getMetaData();
    assertNotNull(meta);
    Schema schema = scoreMeta.getSchema();
    assertEquals(schema.getColumnNum(), meta.getColumnCount());
    for (int i = 0; i < meta.getColumnCount(); i++) {
      assertEquals(schema.getColumn(i).getColumnName(), meta.getColumnName(i + 1));
      assertEquals(schema.getColumn(i).getTableName(), meta.getTableName(i + 1));
      assertEquals(schema.getColumn(i).getDataType().getClass().getCanonicalName(),
          meta.getColumnTypeName(i + 1));
    }

    int i = 0;
    assertTrue(rs.isBeforeFirst());
    for (; rs.next(); i++) {
      assertEquals("test"+i%100, rs.getString(1));
      assertEquals("test"+i%100, rs.getString("deptname"));
      assertEquals(i+1, rs.getInt(2));
      assertEquals(i+1, rs.getInt("score"));
    }
    assertEquals(10000, i);
    assertTrue(rs.isAfterLast());
  }
}
