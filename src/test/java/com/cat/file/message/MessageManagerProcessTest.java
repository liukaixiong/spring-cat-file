package com.cat.file.message;

import com.alibaba.fastjson.JSONObject;
import com.cat.file.message.config.CatFileConfiguration;
import com.cat.file.message.config.NetworkInterfaceManager;
import com.cat.file.message.internal.MessageId;
import com.cat.file.message.model.JsonMessageTree;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;

/**
 * @Module 消息管理
 * @Description 消息管理处理器
 * @Author liukaixiong
 * @Date 2020/11/16 18:07
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {CatFileConfiguration.class})
public class MessageManagerProcessTest extends TestCase {

    @Autowired
    private MessageManagerProcess messageManagerProcess;
    String logId = "liukx-c0a8e901-445933-1478";

    @Test
    public void testInsert() throws InterruptedException {
        MessageId id = MessageId.parse(logId);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("username", "liukaixiong");
        jsonObject.put("password", "lkxlxklxk");
        JsonMessageTree messageTree = new JsonMessageTree("cat", 123, new Date(), jsonObject);
        messageTree.setMessage(jsonObject);
        messageTree.setIpAddress(NetworkInterfaceManager.INSTANCE.getLocalHostAddress());

        messageManagerProcess.insert(messageTree);
        Thread.sleep(500);

        messageManagerProcess.close(id.getHour());
        Thread.sleep(1000);

        testGetMessageObject();
    }

    @Test
    public void testGetMessageObject() {
        JSONObject messageObject = messageManagerProcess.getMessageObject(logId, JSONObject.class);
        System.out.println(messageObject.toJSONString());
    }
}