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

package org.apache.tajo.engine.planner.physical;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.AggFuncCallEval;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.master.ExecutionBlock.PartitionType;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.RangeRetrieverHandler;
import org.apache.tajo.worker.dataserver.retriever.FileChunk;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class TestPhysicalPlanner {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static StorageManager sm;
  private static Path testDir;

  private static TableDesc employee = null;
  private static TableDesc score = null;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();

    util.startCatalogCluster();
    conf = util.getConfiguration();
    testDir = CommonTestingUtil.getTestDir("target/test-data/TestPhysicalPlanner");
    sm = StorageManager.get(conf, testDir);
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.registerFunction(funcDesc);
    }

    Schema schema = new Schema();
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("empId", Type.INT4);
    schema.addColumn("deptName", Type.TEXT);

    Schema schema2 = new Schema();
    schema2.addColumn("deptName", Type.TEXT);
    schema2.addColumn("manager", Type.TEXT);

    Schema scoreSchema = new Schema();
    scoreSchema.addColumn("deptName", Type.TEXT);
    scoreSchema.addColumn("class", Type.TEXT);
    scoreSchema.addColumn("score", Type.INT4);
    scoreSchema.addColumn("nullable", Type.TEXT);

    TableMeta employeeMeta = CatalogUtil.newTableMeta(schema, StoreType.CSV);


    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = StorageManager.getAppender(conf, employeeMeta, employeePath);
    appender.init();
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < 100; i++) {
      tuple.put(new Datum[] {DatumFactory.createText("name_" + i),
          DatumFactory.createInt4(i), DatumFactory.createText("dept_" + i)});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    employee = new TableDescImpl("employee", employeeMeta, employeePath);
    catalog.addTable(employee);

    Path scorePath = new Path(testDir, "score");
    TableMeta scoreMeta = CatalogUtil.newTableMeta(scoreSchema, StoreType.CSV, new Options());
    appender = StorageManager.getAppender(conf, scoreMeta, scorePath);
    appender.init();
    score = new TableDescImpl("score", scoreMeta, scorePath);
    tuple = new VTuple(score.getMeta().getSchema().getColumnNum());
    int m = 0;
    for (int i = 1; i <= 5; i++) {
      for (int k = 3; k < 5; k++) {
        for (int j = 1; j <= 3; j++) {
          tuple.put(
              new Datum[] {
                  DatumFactory.createText("name_" + i), // name_1 ~ 5 (cad: // 5)
                  DatumFactory.createText(k + "rd"), // 3 or 4rd (cad: 2)
                  DatumFactory.createInt4(j), // 1 ~ 3
              m % 3 == 1 ? DatumFactory.createText("one") : NullDatum.get()});
          appender.addTuple(tuple);
          m++;
        }
      }
    }
    appender.flush();
    appender.close();
    catalog.addTable(score);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  private String[] QUERIES = {
      "select name, empId, deptName from employee", // 0
      "select name, empId, e.deptName, manager from employee as e, dept as dp", // 1
      "select name, empId, e.deptName, manager, score from employee as e, dept, score", // 2
      "select p.deptName, sum(score) from dept as p, score group by p.deptName having sum(score) > 30", // 3
      "select p.deptName, score from dept as p, score order by score asc", // 4
      "select name from employee where empId = 100", // 5
      "select deptName, class, score from score", // 6
      "select deptName, class, sum(score), max(score), min(score) from score group by deptName, class", // 7
      "select count(*), max(score), min(score) from score", // 8
      "select count(deptName) from score", // 9
      "select managerId, empId, deptName from employee order by managerId, empId desc", // 10
      "select deptName, nullable from score group by deptName, nullable", // 11
      "select 3 < 4 as ineq, 3.5 * 2 as score", // 12
      "select (1 > 0) and 3 > 1", // 13
      "select deptName, class, sum(score), max(score), min(score) from score", // 14
      "select deptname, class, sum(score), max(score), min(score) from score group by deptname", // 15
      "select name from employee where empid >= 0", // 16
  };

  @Test
  public final void testCreateScanPlan() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "employee", employee.getMeta(),
        employee.getPath(), Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testCreateScanPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil
        .newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(expr);
    LogicalNode rootNode =plan.getRootBlock().getRoot();
    optimizer.optimize(plan);


    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    Tuple tuple;
    int i = 0;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertTrue(tuple.contains(0));
      assertTrue(tuple.contains(1));
      assertTrue(tuple.contains(2));
      i++;
    }
    exec.close();
    assertEquals(100, i);
  }

  @Test
  public final void testCreateScanWithFilterPlan() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "employee", employee.getMeta(),
        employee.getPath(), Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testCreateScanWithFilterPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil
        .newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    Expr expr = analyzer.parse(QUERIES[16]);
    LogicalPlan plan = planner.createPlan(expr);
    LogicalNode rootNode =plan.getRootBlock().getRoot();
    optimizer.optimize(plan);


    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    Tuple tuple;
    int i = 0;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertTrue(tuple.contains(0));
      i++;
    }
    exec.close();
    assertEquals(100, i);
  }

  @Test
  public final void testGroupByPlan() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testGroupByPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(context);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    int i = 0;
    Tuple tuple;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    exec.close();
    assertEquals(10, i);
  }

  @Test
  public final void testHashGroupByPlanWithALLField() throws IOException, PlanningException {
    // TODO - currently, this query does not use hash-based group operator.
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir(
        "target/test-data/testHashGroupByPlanWithALLField");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    Expr expr = analyzer.parse(QUERIES[15]);
    LogicalPlan plan = planner.createPlan(expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    int i = 0;
    Tuple tuple;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertEquals(DatumFactory.createNullDatum(), tuple.get(1));
      assertEquals(12, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    exec.close();
    assertEquals(5, i);
  }

  @Test
  public final void testSortGroupByPlan() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testSortGroupByPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[]{frags[0]}, workDir);
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(context);
    optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan.getRootBlock().getRoot());

    /*HashAggregateExec hashAgg = (HashAggregateExec) exec;

    SeqScanExec scan = (SeqScanExec) hashAgg.getSubOp();

    Column [] grpColumns = hashAgg.getAnnotation().getGroupingColumns();
    QueryBlock.SortSpec [] specs = new QueryBlock.SortSpec[grpColumns.length];
    for (int i = 0; i < grpColumns.length; i++) {
      specs[i] = new QueryBlock.SortSpec(grpColumns[i], true, false);
    }
    SortNode annotation = new SortNode(specs);
    annotation.setInSchema(scan.getSchema());
    annotation.setOutSchema(scan.getSchema());
    SortExec sort = new SortExec(annotation, scan);
    exec = new SortAggregateExec(hashAgg.getAnnotation(), sort);*/

    int i = 0;
    Tuple tuple;
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    assertEquals(10, i);

    exec.rescan();
    i = 0;
    while ((tuple = exec.next()) != null) {
      assertEquals(6, tuple.getInt(2).asInt4()); // sum
      assertEquals(3, tuple.getInt(3).asInt4()); // max
      assertEquals(1, tuple.getInt(4).asInt4()); // min
      i++;
    }
    exec.close();
    assertEquals(10, i);
  }

  private String[] CreateTableAsStmts = {
      "create table grouped1 as select deptName, class, sum(score), max(score), min(score) from score group by deptName, class", // 8
      "create table grouped2 using rcfile as select deptName, class, sum(score), max(score), min(score) from score group by deptName, class", // 8
  };

  @Test
  public final void testStorePlan() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] },
        workDir);
    ctx.setOutputPath(new Path(workDir, "grouped1"));

    Expr context = analyzer.parse(CreateTableAsStmts[0]);
    LogicalPlan plan = planner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);


    TableMeta outputMeta = CatalogUtil.newTableMeta(rootNode.getOutSchema(),
        StoreType.CSV);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    Scanner scanner = StorageManager.getScanner(conf, outputMeta, ctx.getOutputPath());
    scanner.init();
    Tuple tuple;
    int i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    assertEquals(10, i);
    scanner.close();

    // Examine the statistics information
    assertEquals(10, ctx.getResultStats().getNumRows().longValue());
  }

  @Test
  public final void testStorePlanWithRCFile() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testStorePlanWithRCFile");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] },
        workDir);
    ctx.setOutputPath(new Path(workDir, "grouped2"));

    Expr context = analyzer.parse(CreateTableAsStmts[1]);
    LogicalPlan plan = planner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);

    TableMeta outputMeta = CatalogUtil.newTableMeta(rootNode.getOutSchema(),
        StoreType.RCFILE);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    Scanner scanner = StorageManager.getScanner(conf, outputMeta, ctx.getOutputPath());
    scanner.init();
    Tuple tuple;
    int i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    assertEquals(10, i);
    scanner.close();

    // Examine the statistics information
    assertEquals(10, ctx.getResultStats().getNumRows().longValue());
  }

  @Test
  public final void testPartitionedStorePlan() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    QueryUnitAttemptId id = TUtil.newQueryUnitAttemptId();
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testPartitionedStorePlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, id, new Fragment[] { frags[0] },
        workDir);
    Expr context = analyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(context);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    int numPartitions = 3;
    Column key1 = new Column("score.deptName", Type.TEXT);
    Column key2 = new Column("score.class", Type.TEXT);
    StoreTableNode storeNode = new StoreTableNode("partition");
    storeNode.setPartitions(PartitionType.HASH, new Column[]{key1, key2}, numPartitions);
    PlannerUtil.insertNode(rootNode, storeNode);
    rootNode = optimizer.optimize(plan);

    TableMeta outputMeta = CatalogUtil.newTableMeta(rootNode.getOutSchema(),
        StoreType.CSV);

    FileSystem fs = sm.getFileSystem();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    Path path = new Path(workDir, "output");
    FileStatus [] list = fs.listStatus(path);
    assertEquals(numPartitions, list.length);

    Fragment [] fragments = new Fragment[list.length];
    int i = 0;
    for (FileStatus status : list) {
      fragments[i++] = new Fragment("partition", status.getPath(), outputMeta, 0, status.getLen());
    }
    Scanner scanner = new MergeScanner(conf, outputMeta,TUtil.newList(fragments));
    scanner.init();

    Tuple tuple;
    i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(6, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    assertEquals(10, i);
    scanner.close();

    // Examine the statistics information
    assertEquals(10, ctx.getResultStats().getNumRows().longValue());
  }

  @Test
  public final void testPartitionedStorePlanWithEmptyGroupingSet()
      throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    QueryUnitAttemptId id = TUtil.newQueryUnitAttemptId();

    Path workDir = CommonTestingUtil.getTestDir(
        "target/test-data/testPartitionedStorePlanWithEmptyGroupingSet");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, id, new Fragment[] { frags[0] },
        workDir);
    Expr expr = analyzer.parse(QUERIES[14]);
    LogicalPlan plan = planner.createPlan(expr);
    LogicalNode rootNode = plan.getRootBlock().getRoot();
    int numPartitions = 1;
    StoreTableNode storeNode = new StoreTableNode("emptyset");
    storeNode.setPartitions(PartitionType.HASH, new Column[] {}, numPartitions);
    PlannerUtil.insertNode(rootNode, storeNode);
    optimizer.optimize(plan);

    TableMeta outputMeta = CatalogUtil.newTableMeta(rootNode.getOutSchema(),
        StoreType.CSV);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    Path path = new Path(workDir, "output");
    FileSystem fs = sm.getFileSystem();

    FileStatus [] list = fs.listStatus(path);
    assertEquals(numPartitions, list.length);

    Fragment [] fragments = new Fragment[list.length];
    int i = 0;
    for (FileStatus status : list) {
      fragments[i++] = new Fragment("partition", status.getPath(), outputMeta, 0, status.getLen());
    }
    Scanner scanner = new MergeScanner(conf, outputMeta,TUtil.newList(fragments));
    scanner.init();
    Tuple tuple;
    i = 0;
    while ((tuple = scanner.next()) != null) {
      assertEquals(60, tuple.get(2).asInt4()); // sum
      assertEquals(3, tuple.get(3).asInt4()); // max
      assertEquals(1, tuple.get(4).asInt4()); // min
      i++;
    }
    assertEquals(1, i);
    scanner.close();

    // Examine the statistics information
    assertEquals(1, ctx.getResultStats().getNumRows().longValue());
  }

  @Test
  public final void testAggregationFunction() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testAggregationFunction");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    Expr context = analyzer.parse(QUERIES[8]);
    LogicalPlan plan = planner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);

    // Set all aggregation functions to the first phase mode
    GroupbyNode groupbyNode = (GroupbyNode) PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);
    for (Target target : groupbyNode.getTargets()) {
      for (EvalNode eval : EvalTreeUtil.findDistinctAggFunction(target.getEvalTree())) {
        if (eval instanceof AggFuncCallEval) {
          ((AggFuncCallEval) eval).setFirstPhase();
        }
      }
    }

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    exec.init();
    Tuple tuple = exec.next();
    assertEquals(30, tuple.get(0).asInt8());
    assertEquals(3, tuple.get(1).asInt4());
    assertEquals(1, tuple.get(2).asInt4());
    assertNull(exec.next());
    exec.close();
  }

  @Test
  public final void testCountFunction() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testCountFunction");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    Expr context = analyzer.parse(QUERIES[9]);
    LogicalPlan plan = planner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);
    System.out.println(rootNode.toString());

    // Set all aggregation functions to the first phase mode
    GroupbyNode groupbyNode = (GroupbyNode) PlannerUtil.findTopNode(rootNode, NodeType.GROUP_BY);
    for (Target target : groupbyNode.getTargets()) {
      for (EvalNode eval : EvalTreeUtil.findDistinctAggFunction(target.getEvalTree())) {
        if (eval instanceof AggFuncCallEval) {
          ((AggFuncCallEval) eval).setFirstPhase();
        }
      }
    }

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    Tuple tuple = exec.next();
    assertEquals(30, tuple.get(0).asInt8());
    assertNull(exec.next());
    exec.close();
  }

  @Test
  public final void testGroupByWithNullValue() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testGroupByWithNullValue");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    Expr context = analyzer.parse(QUERIES[11]);
    LogicalPlan plan = planner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    int count = 0;
    exec.init();
    while(exec.next() != null) {
      count++;
    }
    exec.close();
    assertEquals(10, count);
  }

  @Test
  public final void testUnionPlan() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "employee", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testUnionPlan");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] { frags[0] }, workDir);
    Expr  context = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);
    LogicalRootNode root = (LogicalRootNode) rootNode;
    UnionNode union = new UnionNode(root.getChild(), root.getChild());
    root.setChild(union);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, root);

    int count = 0;
    exec.init();
    while(exec.next() != null) {
      count++;
    }
    exec.close();
    assertEquals(200, count);
  }

  @Test
  public final void testEvalExpr() throws IOException, PlanningException {
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testEvalExpr");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] { }, workDir);
    Expr expr = analyzer.parse(QUERIES[12]);
    LogicalPlan plan = planner.createPlan(expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    Tuple tuple;
    exec.init();
    tuple = exec.next();
    exec.close();
    assertEquals(true, tuple.get(0).asBool());
    assertTrue(7.0d == tuple.get(1).asFloat8());

    expr = analyzer.parse(QUERIES[13]);
    plan = planner.createPlan(expr);
    rootNode = optimizer.optimize(plan);

    phyPlanner = new PhysicalPlannerImpl(conf, sm);
    exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    tuple = exec.next();
    exec.close();
    assertEquals(DatumFactory.createBool(true), tuple.get(0));
  }

  public final String [] createIndexStmt = {
      "create index idx_employee on employee using bst (name null first, empId desc)"
  };

  //@Test
  public final void testCreateIndex() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "employee", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testCreateIndex");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] {frags[0]}, workDir);
    Expr context = analyzer.parse(createIndexStmt[0]);
    LogicalPlan plan = planner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    while (exec.next() != null) {
    }
    exec.close();

    FileStatus [] list = sm.getFileSystem().listStatus(StorageUtil.concatPath(workDir, "index"));
    assertEquals(2, list.length);
  }

  final static String [] duplicateElimination = {
      "select distinct deptname from score",
  };

  @Test
  public final void testDuplicateEliminate() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "score", score.getMeta(), score.getPath(),
        Integer.MAX_VALUE);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testDuplicateEliminate");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] {frags[0]}, workDir);
    Expr expr = analyzer.parse(duplicateElimination[0]);
    LogicalPlan plan = planner.createPlan(expr);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    Tuple tuple;

    int cnt = 0;
    Set<String> expected = Sets.newHashSet(
        "name_1", "name_2", "name_3", "name_4", "name_5");
    exec.init();
    while ((tuple = exec.next()) != null) {
      assertTrue(expected.contains(tuple.getString(0).asChars()));
      cnt++;
    }
    exec.close();
    assertEquals(5, cnt);
  }

  public String [] SORT_QUERY = {
      "select name, empId from employee order by empId"
  };

  @Test
  public final void testIndexedStoreExec() throws IOException, PlanningException {
    Fragment[] frags = StorageManager.splitNG(conf, "employee", employee.getMeta(),
        employee.getPath(), Integer.MAX_VALUE);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testIndexedStoreExec");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, TUtil.newQueryUnitAttemptId(),
        new Fragment[] {frags[0]}, workDir);
    Expr context = analyzer.parse(SORT_QUERY[0]);
    LogicalPlan plan = planner.createPlan(context);
    LogicalNode rootNode = optimizer.optimize(plan);

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);

    ProjectionExec proj = (ProjectionExec) exec;
    ExternalSortExec sort = (ExternalSortExec) proj.getChild();

    SortSpec[] sortSpecs = sort.getPlan().getSortKeys();
    IndexedStoreExec idxStoreExec = new IndexedStoreExec(ctx, sm, sort, sort.getSchema(), sort.getSchema(), sortSpecs);

    Tuple tuple;
    exec = idxStoreExec;
    exec.init();
    exec.next();
    exec.close();

    Schema keySchema = new Schema();
    keySchema.addColumn("?empId", Type.INT4);
    SortSpec[] sortSpec = new SortSpec[1];
    sortSpec[0] = new SortSpec(keySchema.getColumn(0), true, false);
    TupleComparator comp = new TupleComparator(keySchema, sortSpec);
    BSTIndex bst = new BSTIndex(conf);
    BSTIndex.BSTIndexReader reader = bst.getIndexReader(new Path(workDir, "output/index"),
        keySchema, comp);
    reader.open();
    Path outputPath = StorageUtil.concatPath(workDir, "output", "output");
    TableMeta meta = CatalogUtil.newTableMeta(rootNode.getOutSchema(), StoreType.CSV, new Options());
    SeekableScanner scanner = (SeekableScanner)
        StorageManager.getScanner(conf, meta, outputPath);
    scanner.init();

    int cnt = 0;
    while(scanner.next() != null) {
      cnt++;
    }
    scanner.reset();

    assertEquals(100 ,cnt);

    Tuple keytuple = new VTuple(1);
    for(int i = 1 ; i < 100 ; i ++) {
      keytuple.put(0, DatumFactory.createInt4(i));
      long offsets = reader.find(keytuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]" , ("name_" + i).equals(tuple.get(0).asChars()));
      assertTrue("[seek check " + (i) + " ]" , i == tuple.get(1).asInt4());
    }


    // The below is for testing RangeRetrieverHandler.
    RangeRetrieverHandler handler = new RangeRetrieverHandler(
        new File(new Path(workDir, "output").toUri()), keySchema, comp);
    Map<String,List<String>> kvs = Maps.newHashMap();
    Tuple startTuple = new VTuple(1);
    startTuple.put(0, DatumFactory.createInt4(50));
    kvs.put("start", Lists.newArrayList(
        new String(Base64.encodeBase64(
            RowStoreUtil.RowStoreEncoder.toBytes(keySchema, startTuple), false))));
    Tuple endTuple = new VTuple(1);
    endTuple.put(0, DatumFactory.createInt4(80));
    kvs.put("end", Lists.newArrayList(
        new String(Base64.encodeBase64(
            RowStoreUtil.RowStoreEncoder.toBytes(keySchema, endTuple), false))));
    FileChunk chunk = handler.get(kvs);

    scanner.seek(chunk.startOffset());
    keytuple = scanner.next();
    assertEquals(50, keytuple.get(1).asInt4());

    long endOffset = chunk.startOffset() + chunk.length();
    while((keytuple = scanner.next()) != null && scanner.getNextOffset() <= endOffset) {
      assertTrue(keytuple.get(1).asInt4() <= 80);
    }

    scanner.close();
  }
}