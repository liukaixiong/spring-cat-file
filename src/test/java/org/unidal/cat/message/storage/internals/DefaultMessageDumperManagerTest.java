package org.unidal.cat.message.storage.internals;

import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.unidal.cat.message.config.BeanConfig;
import org.unidal.cat.message.config.NetworkInterfaceManager;
import org.unidal.cat.message.internal.DefaultMessageTree;
import org.unidal.cat.message.internal.MessageId;
import org.unidal.cat.message.internal.MessageTree;
import org.unidal.cat.message.storage.*;
import sun.plugin2.message.Message;

import java.io.IOException;
import java.nio.charset.Charset;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {BeanConfig.class})
public class DefaultMessageDumperManagerTest {

    @Autowired
    private MessageDumperManager messageDumperManager;

    @Autowired
    private MessageFinderManager m_finderManager;

    @Autowired
    private BucketManager m_bucketManager;

    @Before
    public void init() {
    }

    @org.junit.Test
    public void find() throws Exception {

    }

    @org.junit.Test
    public void findOrCreate() throws Exception {
    }

    @org.junit.Test
    public void initialize() throws Exception {

    }

    @Test
    public void storeMsg() throws Exception {
        String logId = "cat-c0a8e901-445933-1478";

        String treeJson = "{\"buffer\":{\"direct\":true,\"readable\":false,\"writable\":false},\"domain\":\"cat\",\"events\":[{\"completed\":false,\"data\":\"[\\\"user-config\\\"]\",\"name\":\"SELECT\",\"status\":\"0\",\"success\":true,\"timestamp\":1605358384660,\"type\":\"SQL.Method\"},{\"completed\":false,\"data\":\"\",\"name\":\"jdbc:mysql://127.0.0.1:3306/cat?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&socketTimeout=120000\",\"status\":\"0\",\"success\":true,\"timestamp\":1605358384661,\"type\":\"SQL.Database\"}],\"formatMessageId\":{\"domain\":\"cat\",\"hour\":445932,\"index\":7981,\"ipAddress\":\"192.168.233.1\",\"ipAddressInHex\":\"c0a8e901\",\"ipAddressValue\":-1062672127,\"timestamp\":1605355200000},\"heartbeats\":[],\"hitSample\":false,\"hostName\":\"ZX-201608302113\",\"ipAddress\":\"192.168.233.1\",\"message\":{\"children\":[{\"$ref\":\"$.events[0]\"},{\"$ref\":\"$.events[1]\"}],\"completed\":false,\"data\":\"SELECT ffffffffffffffff c.id,c.`name`,c.content,c.creation_date,c.modify_date FROM config c WHERE c.name = ?\",\"durationInMicros\":650943,\"durationInMillis\":650,\"name\":\"config.findByName\",\"standalone\":false,\"status\":\"0\",\"success\":true,\"timestamp\":1605358384020,\"type\":\"SQL\"},\"messageId\":\"cat-c0a8e901-445933-7981\",\"metrics\":[],\"processLoss\":false,\"sessionToken\":\"\",\"threadGroupName\":\"RMI Runtime\",\"threadId\":\"20\",\"threadName\":\"RMI TCP Connection(3)-127.0.0.1\",\"transactions\":[{\"$ref\":\"$.message\"}]}";
        DefaultMessageTree tree = JSON.parseObject(treeJson, DefaultMessageTree.class);
        tree.setMessageId(logId);
        tree.setFormatMessageId(null);
        MessageId id = tree.getFormatMessageId();
//        MessageId id = process(logId, tree);
        Thread.sleep(1000);

        ByteBuf byteBuf = m_finderManager.find(tree.getFormatMessageId());
//        ByteBuf byteBuf = null;
        if (byteBuf == null) {
            Bucket bucket = m_bucketManager
                    .getBucket(id.getDomain(), NetworkInterfaceManager.INSTANCE.getLocalHostAddress(), id.getHour(), false);

            if (bucket != null) {
                bucket.flush();
                byteBuf = bucket.get(id);
            }
        }
        if (byteBuf != null) {
            int bytebufLength = byteBuf.readInt();
            System.out.println("反查出来啦! 长度:" + bytebufLength + " 内容" + byteBuf.toString(Charset.defaultCharset()));
        }
        Thread.sleep(2000);

//        process("cat-c0a8e901-445933-1479", tree);

//        messageDumperManager.close(id.getHour());

        System.in.read();
    }

    private MessageId process(String logId, DefaultMessageTree tree) {
        tree.setFormatMessageId(null);
        tree.setMessageId(logId);

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
        return id;
    }

}
