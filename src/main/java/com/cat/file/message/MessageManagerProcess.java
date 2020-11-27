package com.cat.file.message;

import com.cat.file.message.config.NetworkInterfaceManager;
import com.cat.file.message.internal.MessageId;
import com.cat.file.message.internal.MessageTree;
import com.cat.file.message.serialize.MessageCodec;
import com.cat.file.message.storage.*;
import com.cat.file.message.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @Module 发送器
 * @Description 消息存储发送器
 * @Author liukaixiong
 * @Date 2020/11/16 15:48
 */
public class MessageManagerProcess implements ApplicationContextAware, InitializingBean, Runnable {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ApplicationContext applicationContext;

    private MessageFinderManager finderManager;

    private BucketManager bucketManager;

    private MessageDumperManager dumperManager;

    private MessageCodec messageCodec;

    private volatile boolean isActive = true;

    private volatile long currentHours;

    private Set<Integer> currentHourList = new HashSet<Integer>();


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * 插入消息
     *
     * @param messageTree
     */
    public void insert(MessageTree messageTree) {
        MessageId id = messageTree.getFormatMessageId();
        MessageDumper messageDumper = dumperManager.findOrCreate(id.getHour());
        if (messageDumper != null) {
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(4 * 1024);
            // 第一位代表长度，先占位
            buf.writeInt(0);

            byte[] encode = messageCodec.encode(messageTree.getMessage());
            buf.writeBytes(encode);

            // 获取数据的长度
            int msgLength = buf.readableBytes();

            // 重新设置数据的长度
            buf.setInt(0, msgLength - 4);

            messageTree.setBuffer(buf);

            messageDumper.process(messageTree);

            currentHourList.add(id.getHour());
        }
    }

    /**
     * 获取消息
     *
     * @param messageId
     * @return
     */
    public String getMessage(String messageId) {
        return getMessageObject(messageId, String.class);
    }

    /**
     * 获取消息
     *
     * @param messageId
     * @return
     */
    public <T> T getMessageObject(String messageId, Class<T> clazz) {
        try {
            MessageId id = MessageId.parse(messageId);
            if (finderManager != null) {
                ByteBuf byteBuf = this.finderManager.find(id);
                if (byteBuf == null) {
                    Bucket bucket = this.bucketManager.getBucket(id.getDomain(), NetworkInterfaceManager.INSTANCE.getLocalHostAddress(), id.getHour(), false);
                    if (bucket != null) {
                        bucket.flush();
                        byteBuf = bucket.get(id);
                    }
                }

                if (byteBuf != null) {
                    // 前4位是数据长度
                    int byteBufLength = byteBuf.readInt();
                    return messageCodec.decode(byteBuf, clazz);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 存储落盘
     *
     * @param hour
     */
    public void close(int hour) {
        this.finderManager.close(hour);
        this.dumperManager.close(hour);
        logger.info("执行落盘操作 : " + hour);
    }

    /**
     * 存储当前所有数据
     */
    public void closeAll() {
        storeAllDisk();
    }

    @Override
    public void run() {
        logger.info("【【【【【开启定时落盘线程】】】】】");
        while (this.isActive) {
            try {
                long hours = TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis());
                if (hours > this.currentHours) {
                    storeDisk(hours);
                    this.currentHours = hours;
                }
            } catch (Exception e) {
                logger.error("close失败", e);
            } finally {
                Threads.sleep(60 * 1000);
            }
        }
    }

    private void storeAllDisk() {
        storeDisk(null);
    }

    private synchronized void storeDisk(Long hours) {
        this.currentHourList.forEach((hour) -> {
            if (hours == null || hour < hours) {
                close(hour);
            }
        });
        currentHourList.clear();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.finderManager = this.applicationContext.getBean(MessageFinderManager.class);
        this.bucketManager = this.applicationContext.getBean(BucketManager.class);
        this.dumperManager = this.applicationContext.getBean(MessageDumperManager.class);
        this.messageCodec = this.applicationContext.getBean(MessageCodec.class);

        this.currentHours = TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis());

        Threads.forGroup("Cat-File").start(this);
    }

}
