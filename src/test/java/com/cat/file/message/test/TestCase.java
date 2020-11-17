package com.cat.file.message.test;

import com.cat.file.message.utils.MessageUtils;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @Module TODO
 * @Description TODO
 * @Author liukaixiong
 * @Date 2020/11/17 13:24
 */
public class TestCase {

    @Test
    public void testHours() {
        long time = System.currentTimeMillis();
        int hour = (int) TimeUnit.MILLISECONDS.toHours(time);
        long day = TimeUnit.MILLISECONDS.toDays(time);
        System.out.println(hour + "\t" + day);
    }

    @Test
    public void testMessageId() {
        String cat = MessageUtils.genMessageId("cat", 12312, new Date());
        System.out.println(cat);
    }
}
