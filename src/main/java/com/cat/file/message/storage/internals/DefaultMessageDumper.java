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

import com.cat.file.message.config.props.StorageProperties;
import com.cat.file.message.storage.BlockDumperManager;
import com.cat.file.message.storage.MessageDumper;
import com.cat.file.message.storage.exception.MessageQueueFullException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import com.cat.file.message.config.ContainerHolder;
import com.cat.file.message.config.ServerStatisticManager;
import com.cat.file.message.internal.MessageId;
import com.cat.file.message.internal.MessageTree;
import com.cat.file.message.storage.BucketManager;
import com.cat.file.message.storage.MessageProcessor;
import com.cat.file.message.utils.LogUtils;
import com.cat.file.message.utils.Threads;
import com.cat.file.message.utils.TimeHelper;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DefaultMessageDumper extends ContainerHolder implements MessageDumper {

    @Autowired
    private BlockDumperManager m_blockDumperManager;

    @Autowired
    private BucketManager m_bucketManager;

    @Autowired
    private ServerStatisticManager m_statisticManager;

    @Autowired
    private StorageProperties storageProperties;

    private List<BlockingQueue<MessageTree>> m_queues = new ArrayList<BlockingQueue<MessageTree>>();

    private List<MessageProcessor> m_processors = new ArrayList<MessageProcessor>();

    private AtomicInteger m_failCount = new AtomicInteger(-1);

    private Logger m_logger = LoggerFactory.getLogger(getClass());
    private long m_total;

    private int m_processThreads;

    @Override
    public void awaitTermination(int hour) throws InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        String date = sdf.format(new Date(hour * TimeHelper.ONE_HOUR));

        m_logger.info("starting close message processor " + date);
        closeMessageProcessor();
        m_logger.info("end close dumper processor " + date);

        m_logger.info("starting close dumper manager " + date);
        m_blockDumperManager.close(hour);
        m_logger.info("end close dumper manager " + date);

        m_logger.info("starting close bucket manager " + date);
        m_bucketManager.closeBuckets(hour);
        m_logger.info("end close bucket manager " + date);
    }

    private void closeMessageProcessor() throws InterruptedException {
        while (true) {
            boolean allEmpty = true;

            for (BlockingQueue<MessageTree> queue : m_queues) {
                if (!queue.isEmpty()) {
                    allEmpty = false;
                    break;
                }
            }

            if (allEmpty) {
                break;
            } else {
                TimeUnit.MILLISECONDS.sleep(1);
            }
        }

        for (MessageProcessor processor : m_processors) {
            processor.shutdown();
            super.release(processor);
        }
    }

    public void enableLogging(Logger logger) {
        m_logger = logger;
    }

    private int getIndex(String key) {
        return (Math.abs(key.hashCode())) % (m_processThreads);
    }

    @Override
    public void initialize(int hour) {
        int processThreads = storageProperties.getMessageProcessorThreads();
        m_processThreads = processThreads;

        for (int i = 0; i < processThreads; i++) {
            BlockingQueue<MessageTree> queue = new ArrayBlockingQueue<MessageTree>(10000);
            MessageProcessor processor = lookup(MessageProcessor.class);

            m_queues.add(queue);
            m_processors.add(processor);

            processor.initialize(hour, i, queue);
            Threads.forGroup("Cat").start(processor);
        }
    }

    @Override
    public void process(MessageTree tree) {
        MessageId id = tree.getFormatMessageId();
        String domain = id.getDomain();
        // hash by ip address and block hash by domain
        // int index = getIndex(id.getDomain());
        int index = getIndex(id.getIpAddressInHex());

        BlockingQueue<MessageTree> queue = m_queues.get(index);
        boolean success = queue.offer(tree);
        if (!success) {
            m_statisticManager.addMessageDumpLoss(1);

            if ((m_failCount.incrementAndGet() % 100) == 0) {
                LogUtils.logError(new MessageQueueFullException("Error when adding message to queue, fails: " + m_failCount));

                m_logger.info("message tree queue is full " + m_failCount + " index " + index);
                // tree.getBuffer().release();
            }
        } else {
            m_statisticManager.addMessageSize(domain, tree.getBuffer().readableBytes());
//			if ((++m_total) % CatConstants.SUCCESS_COUNT == 0) {
//				m_statisticManager.addMessageDump(CatConstants.SUCCESS_COUNT);
//			}
        }
    }
}
