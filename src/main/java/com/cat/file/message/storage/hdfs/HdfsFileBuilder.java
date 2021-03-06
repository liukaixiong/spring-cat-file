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

import com.cat.file.message.storage.FileType;
import com.cat.file.message.storage.PathBuilder;

import java.text.MessageFormat;
import java.util.Date;


public class HdfsFileBuilder implements PathBuilder {


	private HdfsSystemManager m_fileSystemManager;

	@Override
	public String getPath(String domain, Date startTime, String consumerId, FileType type) {
		MessageFormat format;
		String path;

		switch (type) {
		case TOKEN:
			format = new MessageFormat("/{0,date,yyyyMMdd}/{0,date,HH}/{2}.{3}");
			path = format.format(new Object[] { startTime, null, consumerId, type.getExtension() });
			break;
		default:
			format = new MessageFormat("/{0,date,yyyyMMdd}/{0,date,HH}/{1}-{2}.{3}");
			path = format.format(new Object[] { startTime, domain, consumerId, type.getExtension() });
			break;
		}

		return m_fileSystemManager.getBaseDir() + path;
	}
}
