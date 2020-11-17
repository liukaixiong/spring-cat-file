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

import com.cat.file.message.storage.MessageDumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import com.cat.file.message.bean.Initializable;
import com.cat.file.message.bean.InitializationException;
import com.cat.file.message.config.ContainerHolder;
import com.cat.file.message.storage.MessageDumperManager;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class DefaultMessageDumperManager extends ContainerHolder
        implements MessageDumperManager, Initializable {

    private Map<Integer, MessageDumper> m_dumpers = new LinkedHashMap<Integer, MessageDumper>();

    private Logger m_logger = LoggerFactory.getLogger(getClass());

    @Override
    public synchronized void close(int hour) {
        MessageDumper dumper = m_dumpers.remove(hour);

        if (dumper != null) {
            try {
                dumper.awaitTermination(hour);
            } catch (InterruptedException e) {
                // ignore
            }
            super.release(dumper);
        }
    }

    @Override
    public MessageDumper find(int hour) {
        return m_dumpers.get(hour);
    }

    @Override
    public MessageDumper findOrCreate(int hour) {
        MessageDumper dumper = m_dumpers.get(hour);

        if (dumper == null) {
            synchronized (this) {
                dumper = m_dumpers.get(hour);

                if (dumper == null) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    dumper = lookup(MessageDumper.class);
                    dumper.initialize(hour);

                    m_dumpers.put(hour, dumper);
                    m_logger.info("create message dumper " + sdf.format(new Date(TimeUnit.HOURS.toMillis(hour))));
                }
            }
        }

        return dumper;
    }

    @Override
    public void initialize() throws InitializationException {
    }
}
