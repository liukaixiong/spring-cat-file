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
import io.netty.buffer.Unpooled;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.cat.file.message.storage.FileType;
import com.cat.file.message.storage.PathBuilder;
import com.cat.file.message.storage.TokenMapping;
import com.cat.file.message.utils.LogUtils;
import com.cat.file.message.utils.TimeHelper;

import java.io.IOException;
import java.util.*;

/**
	* Supports up to 64K tokens mapping from <code>String</code> to <code>int</code>, or reverse by local file system.
	*/

public class HdfsTokenMapping implements TokenMapping {
	private static final int BLOCK_SIZE = 32 * 1024;

	private static final String MAGIC_CODE = "TokenMapping"; // token mapping


	protected HdfsSystemManager m_manager;

	private PathBuilder m_bulider;

	private FSDataInputStream m_file;

	private List<String> m_tokens = new ArrayList<String>(1024);

	private Map<String, Integer> m_map = new HashMap<String, Integer>(1024);

	private int m_block;

	private ByteBuf m_data;

	private long m_lastAccessTime;

	@Override
	public void close() {
		try {
			m_file.close();
		} catch (IOException e) {
			LogUtils.logError(e);
		}

		m_tokens.clear();
		m_map.clear();
	}

	@Override
	public String find(int index) throws IOException {
		int len = m_tokens.size();

		if (index < len) {
			m_lastAccessTime = System.currentTimeMillis();

			return m_tokens.get(index);
		} else {
			return null;
		}
	}

	@Override
	public long getLastAccessTime() {
		return m_lastAccessTime;
	}

	private void loadFrom(ByteBuf buf, int length) throws IOException {
		int index = m_map.size();

		buf.writerIndex(length);

		while (buf.isReadable(2)) {
			short len = buf.readShort();

			if (len <= 0) {
				break;
			}

			byte[] data = new byte[len];

			buf.readBytes(data, 0, len);

			String token = new String(data, 0, len, "utf-8");

			m_tokens.add(token);
			m_map.put(token, index++);
		}

		buf.writerIndex(buf.readerIndex());
	}

	@Override
	public int map(String token) throws IOException {
		throw new RuntimeException("unsupport operation");
	}

	@Override
	public void open(int hour, String ip) throws IOException {
		String path = m_bulider.getPath(null, new Date(hour * TimeHelper.ONE_HOUR), ip, FileType.TOKEN);
		FileSystem fs = m_manager.getFileSystem();
		m_file = fs.open(new Path(path));

		m_data = Unpooled.buffer(BLOCK_SIZE);
		m_block = 0;

		while (true) {
			m_file.seek(1L * m_block * BLOCK_SIZE);

			int size = m_file.read(m_data.array());

			if (size < 0) {
				break;
			}

			loadFrom(m_data, size);
			m_data.clear();
			m_data.setZero(0, m_data.capacity());
			m_block++;
		}

		if (m_map.isEmpty()) {
			m_data.writeShort(MAGIC_CODE.length());
			m_data.writeBytes(MAGIC_CODE.getBytes());
			m_tokens.add(MAGIC_CODE);
			m_map.put(MAGIC_CODE, 0);
		}
	}

}
