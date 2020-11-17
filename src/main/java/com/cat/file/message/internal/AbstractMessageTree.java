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
package com.cat.file.message.internal;

import com.cat.file.message.config.NetworkInterfaceManager;
import com.cat.file.message.utils.MessageUtils;
import io.netty.buffer.ByteBuf;

import java.util.Date;

public abstract class AbstractMessageTree<T> implements MessageTree<T> {

    private ByteBuf buf;

    private String domain;

    private String ipAddress;

    private String messageId;

    private MessageId formatMessageId;

    private T message;

    private Date createTime;

    private Integer id;

    public AbstractMessageTree(String domain, String ipAddress, Integer id, Date createTime, T message) {
        this.domain = domain;
        this.ipAddress = ipAddress;
        this.message = message;
        this.createTime = createTime;
        this.id = id;
    }

    public AbstractMessageTree(String domain, Integer id, Date createTime, T message) {
        this.domain = domain;
        this.ipAddress = NetworkInterfaceManager.INSTANCE.getLocalHostAddress();
        this.message = message;
        this.createTime = createTime;
        this.id = id;
    }

    public ByteBuf getBuf() {
        return buf;
    }

    public void setBuf(ByteBuf buf) {
        this.buf = buf;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public ByteBuf getBuffer() {
        return buf;
    }

    @Override
    public void setBuffer(ByteBuf buf) {
        this.buf = buf;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    @Override
    public MessageId getFormatMessageId() {
        String messageId = getMessageId();
        if (messageId != null) {
            this.formatMessageId = MessageId.parse(messageId);
        }
        return this.formatMessageId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @Override
    public String getMessageId() {
        if (this.messageId == null) {
            return MessageUtils.genMessageId(this.domain, this.ipAddress, this.id, this.createTime);
        }
        return this.messageId;
    }

}
