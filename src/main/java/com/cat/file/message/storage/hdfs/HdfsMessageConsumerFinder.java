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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import com.cat.file.message.utils.LogUtils;
import com.cat.file.message.utils.TimeHelper;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;


public class HdfsMessageConsumerFinder implements MessageConsumerFinder {


	private HdfsSystemManager m_fileSystemManager;

	private Map<String, Set<String>> m_caches = new HashMap<String, Set<String>>();

	@Override
	public Set<String> findConsumerIps(final String domain, int hour) {
		String key = domain + '-' + hour;
		Set<String> ips = m_caches.get(key);

		if (ips == null) {
			synchronized (m_caches) {
				ips = m_caches.get(key);

				if (ips == null) {
					ips = findfromHdfs(domain, hour);
					m_caches.put(key, ips);
				}
			}
		}

		return ips;
	}

	private Set<String> findfromHdfs(final String domain, int hour) {
		Date start = new Date(hour * TimeHelper.ONE_HOUR);
		MessageFormat format = new MessageFormat("/{0,date,yyyyMMdd}/{0,date,HH}");
		String parent = m_fileSystemManager.getBaseDir() + format.format(new Object[] { start });

		FileSystem fs;

		try {
			fs = m_fileSystemManager.getFileSystem();
		} catch (IOException e) {
			LogUtils.logError(e);
			return null;
		}

		final Set<String> result = new HashSet<String>();

		try {
			final Path basePath = new Path(parent);

			if (fs != null) {
				fs.listStatus(basePath, new PathFilter() {
					@Override
					public boolean accept(Path p) {
						String name = p.getName();

						if (name.contains(domain) && name.endsWith(".dat")) {
							int start = name.lastIndexOf('-');
							int end = name.length() - 4;

							result.add(name.substring(start + 1, end));
						}
						return false;
					}
				});
			}
		} catch (IOException e) {
			LogUtils.logError(e);
		}
		return result;
	}

}
