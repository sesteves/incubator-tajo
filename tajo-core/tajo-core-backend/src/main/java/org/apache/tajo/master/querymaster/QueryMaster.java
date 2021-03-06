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

package org.apache.tajo.master.querymaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.tajo.QueryConf;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.engine.planner.global.GlobalOptimizer;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.GlobalPlanner;
import org.apache.tajo.master.TajoAsyncDispatcher;
import org.apache.tajo.master.event.QueryStartEvent;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.worker.TajoWorker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO - when exception, send error status to QueryJobManager
public class QueryMaster extends CompositeService implements EventHandler {
  private static final Log LOG = LogFactory.getLog(QueryMaster.class.getName());

  private static int QUERY_SESSION_TIMEOUT = 60 * 1000;  //60 sec

  private Clock clock;

  private TajoAsyncDispatcher dispatcher;

  private GlobalPlanner globalPlanner;

  private GlobalOptimizer globalOptimizer;

//  private boolean isCreateTableStmt;
  private StorageManager storageManager;

  private QueryConf queryConf;

  private Map<QueryId, QueryMasterTask> queryMasterTasks = new HashMap<QueryId, QueryMasterTask>();

  private ClientSessionTimeoutCheckThread clientSessionTimeoutCheckThread;

  private AtomicBoolean queryMasterStop = new AtomicBoolean(false);

  private QueryMasterContext queryMasterContext;

  private QueryHeartbeatThread queryHeartbeatThread;

  private TajoWorker.WorkerContext workerContext;

  public QueryMaster(TajoWorker.WorkerContext workerContext) {
    super(QueryMaster.class.getName());
    this.workerContext = workerContext;
  }

  public void init(Configuration conf) {
    LOG.info("QueryMaster init");
    try {
      queryConf = new QueryConf(conf);
      queryConf.addResource(new Path(QueryConf.QUERY_MASTER_FILENAME));

      QUERY_SESSION_TIMEOUT = 60 * 1000;//queryConf.getIntVar(TajoConf.ConfVars.QUERY_SESSION_TIMEOUT);
      queryMasterContext = new QueryMasterContext(queryConf);

      clock = new SystemClock();

      this.dispatcher = new TajoAsyncDispatcher("querymaster_" + System.currentTimeMillis());
      addIfService(dispatcher);

      this.storageManager = new StorageManager(queryConf);

      globalPlanner = new GlobalPlanner(queryConf, storageManager, dispatcher.getEventHandler());
      globalOptimizer = new GlobalOptimizer();

      dispatcher.register(QueryStartEvent.EventType.class, new QueryStartEventHandler());

    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw new RuntimeException(t);
    }
    super.init(conf);
  }

  @Override
  public void start() {
    LOG.info("====>QueryMaster start");

    queryHeartbeatThread = new QueryHeartbeatThread();
    queryHeartbeatThread.start();

    clientSessionTimeoutCheckThread = new ClientSessionTimeoutCheckThread();
    clientSessionTimeoutCheckThread.start();

    super.start();
  }

  @Override
  public void stop() {
    synchronized(queryMasterStop) {
      if(queryMasterStop.get()) {
         return;
      }

      queryMasterStop.set(true);
      queryMasterStop.notifyAll();
    }

    if(queryHeartbeatThread != null) {
      queryHeartbeatThread.interrupt();
    }

    if(clientSessionTimeoutCheckThread != null) {
      clientSessionTimeoutCheckThread.interrupt();
    }
    super.stop();

    LOG.info("QueryMaster stop");
    if(!queryMasterContext.getWorkerContext().isStandbyMode()) {
      queryMasterContext.getWorkerContext().stopWorker(true);
    }
  }

  public void reportQueryStatusToQueryMaster(QueryId queryId, TajoProtos.QueryState state) {
    LOG.info("Send QueryMaster Ready to QueryJobManager:" + queryId);
    try {
      TajoMasterProtocol.TajoHeartbeat.Builder queryHeartbeatBuilder = TajoMasterProtocol.TajoHeartbeat.newBuilder()
          .setTajoWorkerHost(workerContext.getTajoWorkerManagerService().getBindAddr().getHostName())
          .setTajoWorkerPort(workerContext.getTajoWorkerManagerService().getBindAddr().getPort())
          .setTajoWorkerClientPort(workerContext.getTajoWorkerClientService().getBindAddr().getPort())
          .setState(state)
          .setQueryId(queryId.getProto());

      workerContext.getTajoMasterRpcClient().heartbeat(null, queryHeartbeatBuilder.build(), NullCallback.get());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  @Override
  public void handle(Event event) {
    dispatcher.getEventHandler().handle(event);
  }

  public Query getQuery(QueryId queryId) {
    return queryMasterTasks.get(queryId).getQuery();
  }

  public QueryMasterTask getQueryMasterTask(QueryId queryId) {
    return queryMasterTasks.get(queryId);
  }

  public QueryMasterContext getContext() {
    return this.queryMasterContext;
  }

  public class QueryMasterContext {
    private QueryConf conf;

    public QueryMasterContext(QueryConf conf) {
      this.conf = conf;
    }

    public QueryConf getConf() {
      return conf;
    }

    public TajoAsyncDispatcher getDispatcher() {
      return dispatcher;
    }

    public Clock getClock() {
      return clock;
    }

    public StorageManager getStorageManager() {
      return storageManager;
    }

    public QueryMaster getQueryMaster() {
      return QueryMaster.this;
    }

    public GlobalPlanner getGlobalPlanner() {
      return globalPlanner;
    }
    public GlobalOptimizer getGlobalOptimizer() {
      return globalOptimizer;
    }

    public TajoWorker.WorkerContext getWorkerContext() {
      return workerContext;
    }

    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    public void stopQuery(QueryId queryId) {
      QueryMasterTask queryMasterTask;
      synchronized(queryMasterTasks) {
        queryMasterTask = queryMasterTasks.remove(queryId);
      }
      if(queryMasterTask != null) {
        try {
          queryMasterTask.stop();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      } else {
        LOG.warn("No query info:" + queryId);
      }
      if(!workerContext.isStandbyMode()) {
        stop();
      }
    }
  }

  private class QueryStartEventHandler implements EventHandler<QueryStartEvent> {
    @Override
    public void handle(QueryStartEvent event) {
      LOG.info("====>Start QueryStartEventHandler:" + event.getQueryId());
      //To change body of implemented methods use File | Settings | File Templates.
      QueryMasterTask queryMasterTask = new QueryMasterTask(queryMasterContext,
          event.getQueryId(), event.getLogicalPlanJson());

      queryMasterTask.init(queryConf);
      queryMasterTask.start();
      synchronized(queryMasterTasks) {
        queryMasterTasks.put(event.getQueryId(), queryMasterTask);
      }
    }
  }

  class QueryHeartbeatThread extends Thread {
    public QueryHeartbeatThread() {
      super("QueryHeartbeatThread");
    }

    @Override
    public void run() {
      LOG.info("Start QueryMaster heartbeat thread");
      while(!queryMasterStop.get()) {
        //TODO report all query status
        List<QueryMasterTask> tempTasks = new ArrayList<QueryMasterTask>();
        synchronized(queryMasterTasks) {
          tempTasks.addAll(queryMasterTasks.values());
        }
        synchronized(queryMasterTasks) {
          for(QueryMasterTask eachTask: tempTasks) {
            TajoMasterProtocol.TajoHeartbeat queryHeartbeat = TajoMasterProtocol.TajoHeartbeat.newBuilder()
                .setTajoWorkerHost(workerContext.getTajoWorkerManagerService().getBindAddr().getHostName())
                .setTajoWorkerPort(workerContext.getTajoWorkerManagerService().getBindAddr().getPort())
                .setTajoWorkerClientPort(workerContext.getTajoWorkerClientService().getBindAddr().getPort())
                .setState(eachTask.getState())
                .setQueryId(eachTask.getQueryId().getProto())
                .build();

            workerContext.getTajoMasterRpcClient().heartbeat(null, queryHeartbeat, NullCallback.get());
          }
        }
        synchronized(queryMasterStop) {
          try {
            queryMasterStop.wait(2000);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
      LOG.info("QueryMaster heartbeat thread stopped");
    }
  }


  class ClientSessionTimeoutCheckThread extends Thread {
    public void run() {
      LOG.info("ClientSessionTimeoutCheckThread started");
      while(true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        }
        List<QueryMasterTask> tempTasks = new ArrayList<QueryMasterTask>();
        synchronized(queryMasterTasks) {
          tempTasks.addAll(queryMasterTasks.values());
        }

        for(QueryMasterTask eachTask: tempTasks) {
          try {
            long lastHeartbeat = eachTask.getLastClientHeartbeat();
            long time = System.currentTimeMillis() - lastHeartbeat;
            if(lastHeartbeat > 0 && time > QUERY_SESSION_TIMEOUT) {
              LOG.warn("Query " + eachTask.getQueryId() + " stopped cause query sesstion timeout: " + time + " ms");
              eachTask.expiredSessionTimeout();
            }
          } catch (Exception e) {
            LOG.error(eachTask.getQueryId() + ":" + e.getMessage(), e);
          }
        }
      }
    }
  }

}
