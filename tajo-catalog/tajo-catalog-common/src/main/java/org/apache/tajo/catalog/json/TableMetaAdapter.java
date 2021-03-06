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

package org.apache.tajo.catalog.json;

import com.google.common.base.Preconditions;
import com.google.gson.*;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.TableMetaImpl;
import org.apache.tajo.json.GsonSerDerAdapter;

import java.lang.reflect.Type;

public class TableMetaAdapter implements GsonSerDerAdapter<TableMeta> {

	@Override
	public TableMeta deserialize(JsonElement json, Type typeOfT,
			JsonDeserializationContext context) throws JsonParseException {
		JsonObject jsonObject = json.getAsJsonObject();
		String typeName = jsonObject.get("type").getAsJsonPrimitive().getAsString();
    Preconditions.checkState(typeName.equals("TableMeta"));
		return context.deserialize(jsonObject.get("body"), TableMetaImpl.class);
	}

	@Override
	public JsonElement serialize(TableMeta src, Type typeOfSrc,
			JsonSerializationContext context) {
		JsonObject jsonObj = new JsonObject();
		jsonObj.addProperty("type", "TableMeta");
		JsonElement jsonElem = context.serialize(src, TableMetaImpl.class);
		jsonObj.add("body", jsonElem);
		return jsonObj;
	}

}
