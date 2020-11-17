/*
 * Copyright (c) 2011-2018, Meituan Dianping. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cat.file.message.storage.hdfs;

import com.cat.file.message.bean.Pair;
import com.cat.file.message.config.ContainerHolder;
import com.cat.file.message.storage.TokenMapping;
import com.cat.file.message.storage.TokenMappingManager;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class HdfsTokenMappingManager extends ContainerHolder implements TokenMappingManager {
	private Map<Pair<Integer, String>, TokenMapping> m_cache = new LinkedHashMap<Pair<Integer, String>, TokenMapping>() {

		private static final long serialVersionUID = 1L;

		@Override
		protected boolean removeEldestEntry(Entry<Pair<Integer, String>, TokenMapping> eldest) {
			return size() > 100;
		}

	};

	@Override
	public void close(int hour) {
		Set<Pair<Integer, String>> removes = new HashSet<Pair<Integer, String>>();

		for (Entry<Pair<Integer, String>, TokenMapping> entry : m_cache.entrySet()) {
			Pair<Integer, String> entryKey = entry.getKey();
			Integer key = entryKey.getKey();

			if (key <= hour) {
				removes.add(entryKey);
			}
		}

		for (Pair<Integer, String> pair : removes) {
			TokenMapping mapping = null;

			synchronized (this) {
				mapping = m_cache.remove(pair);
			}

			if (mapping != null) {
				mapping.close();
			}
			super.release(mapping);
		}
	}

	@Override
	public TokenMapping getTokenMapping(int hour, String ip) throws IOException {
		Pair<Integer, String> pair = new Pair<Integer, String>(hour, ip);
		TokenMapping mapping = m_cache.get(pair);

		if (mapping == null) {
			synchronized (this) {
				mapping = m_cache.get(pair);

				if (mapping == null) {
					mapping = lookup(TokenMapping.class, "hdfs");
					mapping.open(hour, ip);
					m_cache.put(pair, mapping);
					super.release(mapping);
				}
			}
		}

		return mapping;
	}

}
