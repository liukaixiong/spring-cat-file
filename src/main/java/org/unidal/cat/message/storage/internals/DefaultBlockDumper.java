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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.unidal.cat.message.config.ContainerHolder;
import org.unidal.cat.message.config.ServerConfigManager;
import org.unidal.cat.message.config.ServerStatisticManager;
import org.unidal.cat.message.storage.Block;
import org.unidal.cat.message.storage.BlockDumper;
import org.unidal.cat.message.storage.BlockWriter;
import org.unidal.cat.message.storage.exception.BlockQueueFullException;
import org.unidal.cat.message.utils.Cat;
import org.unidal.cat.message.utils.Threads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DefaultBlockDumper extends ContainerHolder implements BlockDumper {

    private Logger m_logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ServerStatisticManager m_statisticManager;

    @Autowired
    private ServerConfigManager m_configManager;

    private List<BlockingQueue<Block>> m_queues = new ArrayList<BlockingQueue<Block>>();

    private List<BlockWriter> m_writers = new ArrayList<BlockWriter>();

    private int m_failCount = -1;

    @Override
    public void awaitTermination() throws InterruptedException {
        int index = 0;

        while (index < 100) {
            boolean allEmpty = true;

            for (BlockingQueue<Block> queue : m_queues) {
                if (!queue.isEmpty()) {
                    allEmpty = false;
                    break;
                }
            }

            if (allEmpty) {
                break;
            }

            TimeUnit.MILLISECONDS.sleep(100);

            index++;
        }

        for (final BlockWriter writer : m_writers) {
            writer.shutdown();
            super.release(writer);
        }
    }

    @Override
    public void dump(Block block) throws IOException {
        String domain = block.getDomain();
        int hash = Math.abs(domain.hashCode());
        int index = hash % m_writers.size();
        BlockingQueue<Block> queue = m_queues.get(index);
        boolean success = queue.offer(block);

        if (!success) {
            m_statisticManager.addBlockLoss(1);

            if ((++m_failCount % 100) == 0) {
                Cat.logError(new BlockQueueFullException("Error when adding block to queue, fails: " + m_failCount));
                m_logger.info("block dump queue is full " + m_failCount + " index:" + index);
            }
        } else {
            m_statisticManager.addBlockTotal(1);
        }
    }


    @Override
    public void initialize(int hour) {
        int threads = m_configManager.getMessageDumpThreads();

        for (int i = 0; i < threads; i++) {
            BlockingQueue<Block> queue = new ArrayBlockingQueue<Block>(10000);
            BlockWriter writer = lookup(BlockWriter.class);

            m_queues.add(queue);
            m_writers.add(writer);

            writer.initialize(hour, i, queue);
            Threads.forGroup("Cat").start(writer);
        }
    }


}
