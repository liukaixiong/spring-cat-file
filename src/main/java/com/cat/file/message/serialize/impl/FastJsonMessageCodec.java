package com.cat.file.message.serialize.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.cat.file.message.serialize.MessageCodec;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang.CharSet;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @Module 序列化
 * @Description fastjson序列化
 * @Author liukaixiong
 * @Date 2020/11/16 17:18
 */
@Component
public class FastJsonMessageCodec implements MessageCodec {

    @Override
    public byte[] encode(Object obj) {
        return JSON.toJSONString(obj).getBytes();
    }

    @Override
    public <T> T decode(ByteBuf byteBuffer, Class<T> clazz) {
        String content = byteBuffer.toString(Charset.forName("UTF-8"));
        if (clazz == String.class) {
            return (T) content;
        }
        return JSON.parseObject(content, clazz);
    }

    public static void main(String[] args) {
        String abc = "aaa";
        String s = JSON.toJSONString(abc);

        String ddd = "ddd";
        JSONObject jsonObject = JSON.parseObject(ddd);
        System.out.println(jsonObject);
    }
}
