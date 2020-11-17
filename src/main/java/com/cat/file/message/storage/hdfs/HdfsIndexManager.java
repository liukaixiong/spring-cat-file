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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.cat.file.message.bean.Initializable;
import com.cat.file.message.bean.InitializationException;
import com.cat.file.message.config.ContainerHolder;
import com.cat.file.message.config.ServerConfigManager;
import com.cat.file.message.internal.MessageId;
import com.cat.file.message.storage.Index;
import com.cat.file.message.utils.LogUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class HdfsIndexManager extends ContainerHolder implements Initializable {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private ServerConfigManager m_configManager;


	private HdfsSystemManager m_fileSystemManager;

	private MessageConsumerFinder m_consumerFinder;

	private Map<String, HdfsIndex> m_buckets = new LinkedHashMap<String, HdfsIndex>() {

		private static final long serialVersionUID = 1L;

		@Override
		protected boolean removeEldestEntry(Entry<String, HdfsIndex> eldest) {
			return size() > 1000;
		}
	};


	@Override
	public void initialize() throws InitializationException {
	}

	public MessageId loadMessage(MessageId id) {
		if (m_configManager.isHdfsOn()) {

			try {
				Set<String> ips = m_consumerFinder.findConsumerIps(id.getDomain(), id.getHour());

				return readMessage(id, ips);
			} catch (RuntimeException e) {
				LogUtils.logError(e);
				throw e;
			} catch (Exception e) {
				LogUtils.logError(e);
			}
		}
		return null;
	}

	private MessageId readMessage(MessageId id, Set<String> ips) {
		for (String ip : ips) {
			String domain = id.getDomain();
			int hour = id.getHour();
			String key = domain + '-' + ip + '-' + hour;

			try {
				HdfsIndex bucket = m_buckets.get(key);

				if (bucket == null) {
					synchronized (m_buckets) {
						bucket = m_buckets.get(key);

						if (bucket == null) {
							bucket = (HdfsIndex) lookup(Index.class, HdfsIndex.ID);

							bucket.initialize(domain, ip, hour);
							m_buckets.put(key, bucket);

							super.release(bucket);
						}
					}
				}

				if (bucket != null) {
					MessageId to = bucket.find(id);

					if (to != null) {
						return to;
					}
				}
			} catch (Exception e) {
				LogUtils.logError(e);
			}
		}
		return null;
	}

}
