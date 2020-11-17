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

import org.slf4j.Logger;
import com.cat.file.message.config.ContainerHolder;
import com.cat.file.message.storage.FileType;
import com.cat.file.message.storage.Index;
import com.cat.file.message.storage.IndexManager;
import com.cat.file.message.storage.PathBuilder;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;


public class LocalIndexManager extends ContainerHolder implements IndexManager {
	protected Logger m_logger;

	private Map<Integer, Map<String, Index>> m_indexes = new LinkedHashMap<Integer, Map<String, Index>>();
	private PathBuilder m_bulider;

	private boolean bucketFilesExsits(String domain, String ip, int hour) {
		long timestamp = hour * 3600 * 1000L;
		Date startTime = new Date(timestamp);
		File indexPath = new File(m_bulider.getPath(domain, startTime, ip, FileType.MAPPING));

		return indexPath.exists();
	}

	@Override
	public void close(int hour) {
		Set<Integer> removed = new HashSet<Integer>();

		for (Entry<Integer, Map<String, Index>> entry : m_indexes.entrySet()) {
			Integer key = entry.getKey();

			if (key <= hour) {
				removed.add(key);
			}
		}

		synchronized (m_indexes) {
			for (Integer i : removed) {
				Map<String, Index> value = m_indexes.remove(i);

				for (Index index : value.values()) {
					index.close();
					super.release(index);
				}
			}
		}
	}

	private Map<String, Index> findOrCreateMap(Map<Integer, Map<String, Index>> map, int hour) {
		Map<String, Index> m = map.get(hour);

		if (m == null) {
			synchronized (map) {
				m = map.get(hour);

				if (m == null) {
					m = new LinkedHashMap<String, Index>();
					map.put(hour, m);
				}
			}
		}

		return m;
	}

	@Override
	public Index getIndex(String domain, String ip, int hour, boolean createIfNotExists) throws IOException {
		Map<String, Index> map = findOrCreateMap(m_indexes, hour);
		Index index = map == null ? null : map.get(domain);
		boolean shouldCreate =
								(createIfNotExists && index == null)	|| (!createIfNotExists && bucketFilesExsits(domain, ip, hour));

		if (shouldCreate) {
			synchronized (map) {
				index = map.get(domain);

				if (index == null) {
					index = lookup(LocalIndex.class);
					index.initialize(domain, ip, hour);
					map.put(domain, index);
				}
			}
		}

		return index;
	}
}
