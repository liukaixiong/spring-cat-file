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
package com.cat.file.hadoop.hdfs;

import com.cat.file.message.bean.PathBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import com.cat.file.message.bean.Initializable;
import com.cat.file.message.bean.InitializationException;
import com.cat.file.message.bean.Pair;
import com.cat.file.message.config.ContainerHolder;
import com.cat.file.message.config.ServerConfigManager;
import com.cat.file.message.internal.MessageId;
import com.cat.file.message.internal.MessageTree;
import com.cat.file.message.storage.MessageBucket;
import com.cat.file.message.storage.MessageBucketManager;
import com.cat.file.message.utils.LogUtils;
import com.cat.file.message.utils.Threads;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HdfsMessageBucketManager extends ContainerHolder implements MessageBucketManager, Initializable {

	public static final String ID = "hdfs";

	public static final String HDFS_BUCKET = "HdfsMessageBucket";

	public static final String HARFS_BUCKET = "HarfsMessageBucket";


	private FileSystemManager m_manager;


	private PathBuilder m_pathBuilder;


	private ServerConfigManager m_serverConfigManager;

	private Map<String, MessageBucket> m_buckets = new ConcurrentHashMap<String, MessageBucket>();

	@Override
	public void archive(long startTime) {
		throw new RuntimeException("not support in hdfs message bucket manager");
	}

	private void closeIdleBuckets() throws IOException {
		long now = System.currentTimeMillis();
		long hour = 3600 * 1000L;
		Set<String> closed = new HashSet<String>();

		for (Map.Entry<String, MessageBucket> entry : m_buckets.entrySet()) {
			MessageBucket bucket = entry.getValue();

			if (now - bucket.getLastAccessTime() >= hour) {
				try {
					bucket.close();
					closed.add(entry.getKey());
				} catch (Exception e) {
					LogUtils.logError(e);
				}
			}
		}
		for (String close : closed) {
			MessageBucket bucket = m_buckets.remove(close);

			release(bucket);
		}
	}

	private List<String> filterFiles(FileSystem fs, MessageId id, final String base, final String path) {
		final List<String> paths = new ArrayList<String>();

		try {
			final Path basePath = new Path(base + path);
			final String key = id.getDomain() + '-' + id.getIpAddress();

			if (fs != null) {
				fs.listStatus(basePath, new PathFilter() {
					@Override
					public boolean accept(Path p) {
						String name = p.getName();

						if (name.contains(key) && !name.endsWith(".idx")) {
							paths.add(path + name);
						}
						return false;
					}
				});
			}
		} catch (IOException e) {
			LogUtils.logError(e);
		}
		return paths;
	}

	@Override
	public void initialize() throws InitializationException {
		if (m_serverConfigManager.isHdfsOn()) {
			Threads.forGroup("cat").start(new IdleChecker());
		}
	}

	private Pair<List<String>, String> loadFileFromHar(MessageId id, Date date) throws IOException {
		FileSystem fs = m_manager.getHarFileSystem(ServerConfigManager.DUMP_DIR, date);
		List<String> paths = filterFiles(fs, id, ".", "");

		return new Pair<List<String>, String>(paths, HARFS_BUCKET);
	}

	private Pair<List<String>, String> loadFileFromHdfs(MessageId id, Date date) throws IOException {
		StringBuilder sb = new StringBuilder();
		String p = m_pathBuilder.getLogviewPath(date, "");
		FileSystem fs = m_manager.getFileSystem(ServerConfigManager.DUMP_DIR, sb);

		List<String> paths = filterFiles(fs, id, sb.toString(), p);

		return new Pair<List<String>, String>(paths, HDFS_BUCKET);
	}

	@Override
	public MessageTree loadMessage(String messageId) {
		if (!m_serverConfigManager.isHdfsOn()) {
			return null;
		}


		try {
			MessageId id = MessageId.parse(messageId);
			Date date = new Date(id.getTimestamp());
			Pair<List<String>, String> pair = null;

			if (m_serverConfigManager.isHarMode()) {
				pair = loadFileFromHar(id, date);
			}

			if (pair == null || pair.getKey().isEmpty()) {
				pair = loadFileFromHdfs(id, date);
			}


//			return readMessage(messageId, date, t, pair.getKey());
			return null; // todo 先不管
		} catch (RuntimeException e) {
			LogUtils.logError(e);
			throw e;
		} catch (Exception e) {
			LogUtils.logError(e);
		}
		return null;
	}

//	private MessageTree readMessage(String messageId, Date date, Transaction t, List<String> paths) {
//		for (String dataFile : paths) {
//			try {
//				String type = null;
//				StringBuilder sb = new StringBuilder();
//
//				sb.append(type).append("-").append(date.toString()).append("-").append(dataFile);
//				String bKey = sb.toString();
//
//				Cat.logEvent(type, bKey);
//				MessageBucket bucket = m_buckets.get(bKey);
//
//				if (bucket == null) {
//					bucket = lookup(MessageBucket.class, type);
//					bucket.initialize(dataFile, date);
//					m_buckets.put(bKey, bucket);
//				}
//
//				MessageTree tree = bucket.findById(messageId);
//
//				if (tree != null && tree.getMessageId().equals(messageId)) {
//					return tree;
//				}
//			} catch (Exception e) {
//				Cat.logError(e);
//			}
//		}
//		return null;
//	}

	@Override
	public void storeMessage(MessageTree tree, MessageId id) {
		throw new UnsupportedOperationException("Not supported by HDFS!");
	}

	class IdleChecker implements Threads.Task {
		@Override
		public String getName() {
			return "HdfsMessageBucketManager-IdleChecker";
		}

		@Override
		public void run() {
			try {
				while (true) {
					Thread.sleep(60 * 1000L); // 1 minute

					try {
						closeIdleBuckets();
					} catch (IOException e) {
						LogUtils.logError(e);
					}
				}
			} catch (InterruptedException e) {
				// ignore it
			}
		}

		@Override
		public void shutdown() {
		}
	}
}
