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
package org.unidal.cat.message.storage.internals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.cat.message.config.ContainerHolder;
import org.unidal.cat.message.storage.BlockDumper;
import org.unidal.cat.message.storage.BlockDumperManager;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class DefaultBlockDumperManager extends ContainerHolder implements BlockDumperManager {

    private Map<Integer, BlockDumper> m_map = new LinkedHashMap<Integer, BlockDumper>();

    private Logger m_logger = LoggerFactory.getLogger(getClass());

    @Override
    public void close(int hour) {
        BlockDumper dumper = m_map.remove(hour);

        if (dumper != null) {
            try {
                dumper.awaitTermination();
                super.release(dumper);
            } catch (InterruptedException e) {
                // ignore it
            }
        }
    }

    @Override
    public BlockDumper findOrCreate(int hour) {
        BlockDumper dumper = m_map.get(hour);

        if (dumper == null) {
            synchronized (this) {
                dumper = m_map.get(hour);

                if (dumper == null) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    dumper = lookup(BlockDumper.class);
                    dumper.initialize(hour);

                    m_map.put(hour, dumper);
                    m_logger.info("Create block dumper " + sdf.format(new Date(TimeUnit.HOURS.toMillis(hour))));
                }
            }
        }

        return dumper;
    }
}
