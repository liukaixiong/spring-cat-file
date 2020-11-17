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

import io.netty.buffer.ByteBuf;
import com.cat.file.message.bean.Initializable;
import com.cat.file.message.bean.InitializationException;
import com.cat.file.message.config.ContainerHolder;
import com.cat.file.message.config.ServerConfigManager;
import com.cat.file.message.handler.CodecHandler;
import com.cat.file.message.internal.MessageId;
import com.cat.file.message.internal.MessageTree;
import com.cat.file.message.storage.Bucket;
import com.cat.file.message.utils.LogUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class HdfsBucketManager extends ContainerHolder implements Initializable  {

	private ServerConfigManager m_configManager;

	private HdfsSystemManager m_fileSystemManager;

	private MessageConsumerFinder m_consumerFinder;

	private Map<String, HdfsBucket> m_buckets = new LinkedHashMap<String, HdfsBucket>() {

		private static final long serialVersionUID = 1L;

		@Override
		protected boolean removeEldestEntry(Entry<String, HdfsBucket> eldest) {
			return size() > 1000;
		}
	};

	@Override
	public void initialize() throws InitializationException {
	}

	public MessageTree loadMessage(MessageId id) {
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

	private MessageTree readMessage(MessageId id, Set<String> ips) {
		for (String ip : ips) {
			String domain = id.getDomain();
			int hour = id.getHour();
			String key = domain + '-' + ip + '-' + hour;

			try {
				HdfsBucket bucket = m_buckets.get(key);

				if (bucket == null) {
					synchronized (m_buckets) {
						bucket = m_buckets.get(key);

						if (bucket == null) {
							bucket = (HdfsBucket) lookup(Bucket.class, HdfsBucket.ID);

							bucket.initialize(domain, ip, hour);
							m_buckets.put(key, bucket);

							super.release(bucket);
						}
					}
				}

				if (bucket != null) {
					ByteBuf data = bucket.get(id);

					if (data != null) {
						try {
							MessageTree tree = CodecHandler.decode(data);

							if (tree.getMessageId().equals(id.toString())) {
								return tree;
							}
						} finally {
							CodecHandler.reset();
						}
					}
				}
			} catch (Exception e) {
				LogUtils.logError(e);
			}
		}
		return null;
	}

}
