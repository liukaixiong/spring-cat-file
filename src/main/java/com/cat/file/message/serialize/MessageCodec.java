package com.cat.file.message.serialize;

import io.netty.buffer.ByteBuf;

/**
 * @Module 序列化
 * @Description 序列化规则定义
 * @Author liukaixiong
 * @Date 2020/11/16 17:14
 */
public interface MessageCodec {

    public byte[] encode(Object obj);

    public <T> T decode(ByteBuf byteBuf, Class<T> clazz);
}
