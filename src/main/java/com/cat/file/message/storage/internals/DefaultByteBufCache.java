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
package com.cat.file.message.storage.internals;

import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import com.cat.file.message.bean.Initializable;
import com.cat.file.message.bean.InitializationException;
import com.cat.file.message.utils.LogUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class DefaultByteBufCache implements ByteBufCache, Initializable {

	private BlockingQueue<ByteBuffer> m_bufs = new ArrayBlockingQueue<ByteBuffer>(8000);

	private Logger m_logger = LoggerFactory.getLogger(getClass());

	private AtomicInteger m_count = new AtomicInteger();

	public ByteBuffer get() {
		ByteBuffer buf = m_bufs.poll();

		if (buf == null) {
			buf = ByteBuffer.allocate(32 * 1024);
		}

		return buf;
	}

	@Override
	public void initialize() throws InitializationException {
	}

	public void put(ByteBuffer buf) {
		byte[] array = buf.array();

		for (int i = 0; i < array.length; i++) {
			array[i] = 0;
		}

		buf.clear();

		boolean result = m_bufs.offer(buf);

		if (!result) {
			try {
				ReferenceCountUtil.release(buf);
			} catch (Exception e) {
				LogUtils.logError(e);
			}

			if (m_count.incrementAndGet() % 100 == 0) {
				m_logger.info("error when put back buf");
			}
		}
	}

}
