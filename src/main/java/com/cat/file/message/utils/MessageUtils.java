package com.cat.file.message.utils;

import com.cat.file.message.config.NetworkInterfaceManager;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.collections.IteratorUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Module 消息处理工具
 * @Description 消息处理
 * @Author liukaixiong
 * @Date 2020/11/17 14:14
 */
public class MessageUtils {
    public static String genMessageId(String applicationName, Integer id, Date createDate) {
        return genMessageId(applicationName, NetworkInterfaceManager.INSTANCE.getLocalHostAddress(), id, createDate);
    }

    /**
     * 生成对应的消息编号
     *
     * @param applicationName 应用名
     * @param ip              ip
     * @param id              递增编号
     * @param createDate      创建时间
     * @return
     */
    public static String genMessageId(String applicationName, String ip, Integer id, Date createDate) {
        StringBuilder sb = new StringBuilder();
        sb.append(applicationName);
        sb.append("-");
        sb.append(getIpHex(ip));
        sb.append("-");
        long time = createDate.getTime();
        sb.append(TimeUnit.MILLISECONDS.toHours(time));
        sb.append("-");
        sb.append(id);
        return sb.toString();
    }

    /**
     * 获取本机ip的转码
     *
     * @return
     */
    public static String getLocalIpHex() {
        String ip = NetworkInterfaceManager.INSTANCE.getLocalHostAddress();

        return getIpHex(ip);
    }

    /**
     * 获取对应的ip转码
     *
     * @param ip
     * @return
     */
    public static String getIpHex(String ip) {

        Iterable<String> itemIterable = Splitter.on(".").omitEmptyStrings().split(ip);

        ArrayList<String> items = Lists.newArrayList(itemIterable);

        byte[] bytes = new byte[4];

        for (int i = 0; i < 4; i++) {
            bytes[i] = (byte) Integer.parseInt(items.get(i));
        }

        StringBuilder sb = new StringBuilder(bytes.length / 2);

        for (byte b : bytes) {
            sb.append(Integer.toHexString((b >> 4) & 0x0F));
            sb.append(Integer.toHexString(b & 0x0F));
        }
        return sb.toString();
    }

}
