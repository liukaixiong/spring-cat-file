package com.cat.file.message.storage.internals;

import com.alibaba.fastjson.JSON;
import com.cat.file.message.storage.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;
import com.cat.file.message.config.CatFileConfiguration;
import com.cat.file.message.config.NetworkInterfaceManager;
import com.cat.file.message.internal.DefaultMessageTree;
import com.cat.file.message.internal.MessageId;

import java.nio.charset.Charset;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {CatFileConfiguration.class})
public class DefaultMessageDumperManagerTest {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private MessageDumperManager messageDumperManager;

    @Autowired
    private MessageFinderManager m_finderManager;

    @Autowired
    private BucketManager m_bucketManager;

    @Before
    public void init() {

    }

    /**
     * 查找内存中的数据
     *
     * @throws Exception
     */
    @Test
    public void testFindMemoryData() throws Exception {
        String logId = "cat-c0a8e901-445933-1478";
        DefaultMessageTree tree = getDefaultMessageTree(logId, "你不懂我，我不怪你。");
        // 先进行存储到内存块中
        storeBlock(logId, tree);
        ByteBuf byteBuf = m_finderManager.find(tree.getFormatMessageId());
        validMsgContent(byteBuf);
    }

    /**
     * 查找本地桶的数据
     *
     * @throws Exception
     */
    @Test
    public void testFindLocalFileData() throws Exception {
        String logId = "efg-c0a8e901-445933-1000000";
        DefaultMessageTree tree = getDefaultMessageTree(logId, "你不懂我，我不怪你。傻啦吧唧");

        // 先进行存储到内存块中
        storeBlock(logId, tree);

        MessageId id = tree.getFormatMessageId();
        // 落地到磁盘
        messageDumperManager.close(id.getHour());

        ByteBuf byteBuf = m_finderManager.find(tree.getFormatMessageId());
        Assert.isNull(byteBuf, "内存中不应该查到");

        // 从桶中查找
        Bucket bucket = m_bucketManager.getBucket(id.getDomain(), NetworkInterfaceManager.INSTANCE.getLocalHostAddress(), id.getHour(), false);
        byteBuf = bucket.get(id);
        validMsgContent(byteBuf);
    }

    private void validMsgContent(ByteBuf byteBuf) {
        if (byteBuf != null) {
            // 由于前4个字节存储的是长度
            int byteBufLength = byteBuf.readInt();
            String data = byteBuf.toString(Charset.defaultCharset());
            logger.info("反查出来啦! 长度:" + byteBufLength + " 内容:" + data);
            Assert.notNull(data, "数据空了");
        }
        Assert.notNull(byteBuf, "数据反查失败");
    }

    private DefaultMessageTree getDefaultMessageTree(String logId, String message) throws Exception {
        String treeJson = "{\"buffer\":{\"direct\":true,\"readable\":false,\"writable\":false},\"domain\":\"cat\",\"events\":[{\"completed\":false,\"data\":\"[\\\"user-config\\\"]\",\"name\":\"SELECT\",\"status\":\"0\",\"success\":true,\"timestamp\":1605358384660,\"type\":\"SQL.Method\"},{\"completed\":false,\"data\":\"\",\"name\":\"jdbc:mysql://127.0.0.1:3306/cat?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&socketTimeout=120000\",\"status\":\"0\",\"success\":true,\"timestamp\":1605358384661,\"type\":\"SQL.Database\"}],\"formatMessageId\":{\"domain\":\"cat\",\"hour\":445932,\"index\":7981,\"ipAddress\":\"192.168.233.1\",\"ipAddressInHex\":\"c0a8e901\",\"ipAddressValue\":-1062672127,\"timestamp\":1605355200000},\"heartbeats\":[],\"hitSample\":false,\"hostName\":\"ZX-201608302113\",\"ipAddress\":\"192.168.233.1\",\"message\":{\"children\":[{\"$ref\":\"$.events[0]\"},{\"$ref\":\"$.events[1]\"}],\"completed\":false,\"data\":\"SELECT ffffffffffffffff c.id,c.`name`,c.content,c.creation_date,c.modify_date FROM config c WHERE c.name = ?\",\"durationInMicros\":650943,\"durationInMillis\":650,\"name\":\"config.findByName\",\"standalone\":false,\"status\":\"0\",\"success\":true,\"timestamp\":1605358384020,\"type\":\"SQL\"},\"messageId\":\"cat-c0a8e901-445933-7981\",\"metrics\":[],\"processLoss\":false,\"sessionToken\":\"\",\"threadGroupName\":\"RMI Runtime\",\"threadId\":\"20\",\"threadName\":\"RMI TCP Connection(3)-127.0.0.1\",\"transactions\":[{\"$ref\":\"$.message\"}]}";
        DefaultMessageTree tree = JSON.parseObject(treeJson, DefaultMessageTree.class);
//        tree.setMessageId(logId);
        tree.setMessage(message);
        return tree;
    }


    @Test
    public void storeMsg() throws Exception {
        String logId = "cat-c0a8e901-445933-1478";

        String treeJson = "{\"buffer\":{\"direct\":true,\"readable\":false,\"writable\":false},\"domain\":\"cat\",\"events\":[{\"completed\":false,\"data\":\"[\\\"user-config\\\"]\",\"name\":\"SELECT\",\"status\":\"0\",\"success\":true,\"timestamp\":1605358384660,\"type\":\"SQL.Method\"},{\"completed\":false,\"data\":\"\",\"name\":\"jdbc:mysql://127.0.0.1:3306/cat?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&socketTimeout=120000\",\"status\":\"0\",\"success\":true,\"timestamp\":1605358384661,\"type\":\"SQL.Database\"}],\"formatMessageId\":{\"domain\":\"cat\",\"hour\":445932,\"index\":7981,\"ipAddress\":\"192.168.233.1\",\"ipAddressInHex\":\"c0a8e901\",\"ipAddressValue\":-1062672127,\"timestamp\":1605355200000},\"heartbeats\":[],\"hitSample\":false,\"hostName\":\"ZX-201608302113\",\"ipAddress\":\"192.168.233.1\",\"message\":{\"children\":[{\"$ref\":\"$.events[0]\"},{\"$ref\":\"$.events[1]\"}],\"completed\":false,\"data\":\"SELECT ffffffffffffffff c.id,c.`name`,c.content,c.creation_date,c.modify_date FROM config c WHERE c.name = ?\",\"durationInMicros\":650943,\"durationInMillis\":650,\"name\":\"config.findByName\",\"standalone\":false,\"status\":\"0\",\"success\":true,\"timestamp\":1605358384020,\"type\":\"SQL\"},\"messageId\":\"cat-c0a8e901-445933-7981\",\"metrics\":[],\"processLoss\":false,\"sessionToken\":\"\",\"threadGroupName\":\"RMI Runtime\",\"threadId\":\"20\",\"threadName\":\"RMI TCP Connection(3)-127.0.0.1\",\"transactions\":[{\"$ref\":\"$.message\"}]}";
        DefaultMessageTree tree = JSON.parseObject(treeJson, DefaultMessageTree.class);
//        tree.setMessageId(logId);
        MessageId id = tree.getFormatMessageId();
//        MessageId id = process(logId, tree);
        Thread.sleep(1000);
        // 基于内存查找
        ByteBuf byteBuf = m_finderManager.find(tree.getFormatMessageId());
        if (byteBuf == null) {
            // 本地文件查找
            Bucket bucket = m_bucketManager
                    .getBucket(id.getDomain(), NetworkInterfaceManager.INSTANCE.getLocalHostAddress(), id.getHour(), false);

            if (bucket != null) {
                bucket.flush();
                byteBuf = bucket.get(id);
            }
        }
        if (byteBuf != null) {
            int byteBufLength = byteBuf.readInt();
            System.out.println("反查出来啦! 长度:" + byteBufLength + " 内容" + byteBuf.toString(Charset.defaultCharset()));
        }
        Thread.sleep(2000);

//        process("cat-c0a8e901-445933-1479", tree);
// 需要注意的是，当前小时的数据如果不执行关闭的话，是不会落盘的。
        messageDumperManager.close(id.getHour());
    }

    /**
     * 执行存储写入本地文件
     *
     * @param logId
     * @param tree
     * @return
     */
    private MessageId storeBlock(String logId, DefaultMessageTree tree) throws Exception {
//        tree.setMessageId(logId);

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(4 * 1024);
        buf.writeInt(0); // 第一位代表长度，先占位
        buf.writeBytes(tree.getMessage().getBytes());
        int msgLength = buf.readableBytes();
        buf.setInt(0, msgLength - 4); // reset the message size
        tree.setBuffer(buf);
        MessageId id = tree.getFormatMessageId();
        // 将内存和文件进行绑定
        MessageDumper messageDumper = messageDumperManager.findOrCreate(id.getHour());
        messageDumper.process(tree);
        // 由于是异步队列，先休眠一秒等异步队列处理完成
        Thread.sleep(500);
        return id;
    }

}
