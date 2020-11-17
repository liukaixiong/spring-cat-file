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
package com.cat.file.message.storage.local;

import com.cat.file.message.bean.Pair;
import com.cat.file.message.config.ContainerHolder;
import com.cat.file.message.storage.TokenMapping;
import com.cat.file.message.storage.TokenMappingManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class LocalTokenMappingManager extends ContainerHolder implements TokenMappingManager {
	private Map<Pair<Integer, String>, TokenMapping> m_cache = new HashMap<Pair<Integer, String>, TokenMapping>();

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
					mapping = lookup(LocalTokenMapping.class);
					mapping.open(hour, ip);
					m_cache.put(pair, mapping);
				}
			}
		}

		return mapping;
	}

}
