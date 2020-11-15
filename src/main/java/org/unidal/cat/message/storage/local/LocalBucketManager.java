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
package org.unidal.cat.message.storage.local;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.cat.message.config.ContainerHolder;
import org.unidal.cat.message.storage.Bucket;
import org.unidal.cat.message.storage.BucketManager;
import org.unidal.cat.message.storage.FileType;
import org.unidal.cat.message.storage.PathBuilder;
import org.unidal.cat.message.utils.Cat;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;


@Component
public class LocalBucketManager extends ContainerHolder implements BucketManager {

	protected Logger m_logger;

	@Autowired
	private PathBuilder m_builder;

	private Map<Integer, Map<String, Bucket>> m_buckets = new LinkedHashMap<Integer, Map<String, Bucket>>();

	private boolean bucketFilesExists(String domain, String ip, int hour) {
		long timestamp = hour * 3600 * 1000L;
		Date startTime = new Date(timestamp);
		File dataPath = new File(m_builder.getPath(domain, startTime, ip, FileType.DATA));
		File indexPath = new File(m_builder.getPath(domain, startTime, ip, FileType.INDEX));

		return dataPath.exists() && indexPath.exists();
	}

	@Override
	public synchronized void closeBuckets(int hour) {
		Set<Integer> removed = new HashSet<Integer>();

		for (Entry<Integer, Map<String, Bucket>> e : m_buckets.entrySet()) {
			int h = e.getKey();

			if (h <= hour) {
				removed.add(h);
			}
		}

		for (Integer h : removed) {
			Map<String, Bucket> buckets = m_buckets.get(h);

			if (buckets != null) {
				for (Bucket bucket : buckets.values()) {
					try {
						bucket.close();
					} catch (Exception e) {
						Cat.logError(e);
					} finally {
						super.release(bucket);
					}
				}
			}
		}

		for (Integer h : removed) {
			m_buckets.remove(h);
		}

	}

	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

	private Map<String, Bucket> findOrCreateMap(Map<Integer, Map<String, Bucket>> map, int hour) {
		Map<String, Bucket> m = map.get(hour);

		if (m == null) {
			synchronized (map) {
				m = map.get(hour);

				if (m == null) {
					m = new LinkedHashMap<String, Bucket>();
					map.put(hour, m);
				}
			}
		}

		return m;
	}

	@Override
	public Bucket getBucket(String domain, String ip, int hour, boolean createIfNotExists) throws IOException {
		Map<String, Bucket> map = findOrCreateMap(m_buckets, hour);
		Bucket bucket = map.get(domain);

		if (bucket == null) {
			boolean shouldCreate = createIfNotExists || bucketFilesExists(domain, ip, hour);

			if (shouldCreate) {
				synchronized (map) {
					bucket = map.get(domain);

					if (bucket == null) {
						bucket = lookup(LocalBucket.class);
						bucket.initialize(domain, ip, hour, createIfNotExists);
						map.put(domain, bucket);
					}
				}
			}
		}

		return bucket;
	}



}
