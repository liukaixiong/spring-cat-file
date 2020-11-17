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

import com.cat.file.message.storage.Block;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import com.cat.file.message.config.NetworkInterfaceManager;
import com.cat.file.message.config.ServerStatisticManager;
import com.cat.file.message.storage.BlockWriter;
import com.cat.file.message.storage.Bucket;
import com.cat.file.message.storage.BucketManager;
import com.cat.file.message.utils.LogUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DefaultBlockWriter implements BlockWriter {

    @Autowired
    private BucketManager m_bucketManager;

    @Autowired
    private ServerStatisticManager m_statisticManager;

    private int m_index;

    private BlockingQueue<Block> m_queue;

    private long m_hour;

    private int m_count;

    private AtomicBoolean m_enabled;

    private CountDownLatch m_latch;

    @Override
    public String getName() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        return getClass().getSimpleName() + " " + sdf.format(new Date(TimeUnit.HOURS.toMillis(m_hour))) + "-" + m_index;
    }

    @Override
    public void initialize(int hour, int index, BlockingQueue<Block> queue) {
        m_hour = hour;
        m_index = index;
        m_queue = queue;
        m_enabled = new AtomicBoolean(true);
        m_latch = new CountDownLatch(1);
    }

    private void processBlock(String ip, Block block) {
        try {
            Bucket bucket = m_bucketManager.getBucket(block.getDomain(), ip, block.getHour(), true);
            boolean monitor = (++m_count) % 1000 == 0;

            if (monitor) {

                try {
                    bucket.puts(block.getData(), block.getOffsets());
                } catch (Exception e) {
                    LogUtils.logError(ip, e);
                }

            } else {
                try {
                    bucket.puts(block.getData(), block.getOffsets());
                } catch (Exception e) {
                    LogUtils.logError(ip, e);
                }
            }
        } catch (Exception e) {
            LogUtils.logError(ip, e);
        } catch (Error e) {
            LogUtils.logError(ip, e);
        } finally {
            block.clear();
        }
    }

    @Override
    public void run() {
        String ip = NetworkInterfaceManager.INSTANCE.getLocalHostAddress();

        try {
            while (m_enabled.get() || !m_queue.isEmpty()) {
                Block block = m_queue.poll(5, TimeUnit.MILLISECONDS);

                if (block != null) {
                    long time = System.currentTimeMillis();
                    processBlock(ip, block);
                    long duration = System.currentTimeMillis() - time;

                    m_statisticManager.addBlockTime(duration);
                }
            }
        } catch (InterruptedException e) {
            // ignore it
        }

        m_latch.countDown();
    }

    @Override
    public void shutdown() {
        m_enabled.set(false);

        try {
            m_latch.await();
        } catch (InterruptedException e) {
            // ignore it
        }
        while (true) {
            Block block = m_queue.poll();

            if (block != null) {
                processBlock(NetworkInterfaceManager.INSTANCE.getLocalHostAddress(), block);
            } else {
                break;
            }
        }
    }

}
