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

package org.apache.tajo.catalog;

import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.common.ProtoObject;

import java.util.Iterator;
import java.util.Map.Entry;

public interface TableMeta extends ProtoObject<TableProto>, Cloneable, GsonObject {
  
  void setStorageType(StoreType storeType);
  
  StoreType getStoreType();
  
  void setSchema(Schema schema);
  
  Schema getSchema();
  
  void putOption(String key, String val);
  
  void setStat(TableStat stat);
  
  String getOption(String key);
  
  String getOption(String key, String defaultValue);
  
  Iterator<Entry<String,String>> getOptions();
  
  TableStat getStat();
  
  Object clone() throws CloneNotSupportedException;
}
