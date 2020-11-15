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
package org.unidal.cat.message.internal;

import io.netty.buffer.ByteBuf;

public class DefaultMessageTree implements MessageTree {

    private ByteBuf m_buf;

    private String m_domain;

    private String m_hostName;

    private String m_ipAddress;

    private String m_messageId;

    private String m_parentMessageId;

    private String m_rootMessageId;

    private String m_sessionToken;

    private String m_threadGroupName;

    private String m_threadId;

    private String m_threadName;

    private MessageId m_formatMessageId;

    private String message;

    private boolean m_discard = true;

    private boolean m_processLoss = false;

    private boolean m_hitSample = false;

    @Override
    public boolean canDiscard() {
        return m_discard;
    }

    @Override
    public MessageTree copy() {
        MessageTree tree = new DefaultMessageTree();
        tree.setDomain(m_domain);
        tree.setHostName(m_hostName);
        tree.setIpAddress(m_ipAddress);
        tree.setMessageId(m_messageId);
        tree.setParentMessageId(m_parentMessageId);
        tree.setRootMessageId(m_rootMessageId);
        tree.setSessionToken(m_sessionToken);
        tree.setThreadGroupName(m_threadGroupName);
        tree.setThreadId(m_threadId);
        tree.setThreadName(m_threadName);
        tree.setDiscardPrivate(m_discard);
        tree.setHitSample(m_hitSample);
        return tree;
    }

    public ByteBuf getBuffer() {
        return m_buf;
    }

    public void setBuffer(ByteBuf buf) {
        m_buf = buf;
    }

    @Override
    public String getMessage() {
        return this.message;
    }

    @Override
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String getDomain() {
        return m_domain;
    }

    @Override
    public void setDomain(String domain) {
        m_domain = domain;
    }

    public MessageId getFormatMessageId() {
        if (m_formatMessageId == null) {
            m_formatMessageId = MessageId.parse(m_messageId);
        }
        return m_formatMessageId;
    }

    public void setFormatMessageId(MessageId formatMessageId) {
        m_formatMessageId = formatMessageId;
    }

    @Override
    public String getHostName() {
        return m_hostName;
    }

    @Override
    public void setHostName(String hostName) {
        m_hostName = hostName;
    }

    @Override
    public String getIpAddress() {
        return m_ipAddress;
    }

    @Override
    public void setIpAddress(String ipAddress) {
        m_ipAddress = ipAddress;
    }

    @Override
    public String getSessionToken() {
        return m_sessionToken;
    }

    @Override
    public void setSessionToken(String sessionToken) {
        m_sessionToken = sessionToken;
    }


    @Override
    public String getMessageId() {
        return m_messageId;
    }

    @Override
    public void setMessageId(String messageId) {
        if (messageId != null && messageId.length() > 0) {
            m_messageId = messageId;
        }
    }

    @Override
    public String getParentMessageId() {
        return m_parentMessageId;
    }

    @Override
    public void setParentMessageId(String parentMessageId) {
        if (parentMessageId != null && parentMessageId.length() > 0) {
            m_parentMessageId = parentMessageId;
        }
    }

    @Override
    public String getRootMessageId() {
        return m_rootMessageId;
    }

    @Override
    public void setRootMessageId(String rootMessageId) {
        if (rootMessageId != null && rootMessageId.length() > 0) {
            m_rootMessageId = rootMessageId;
        }
    }

    @Override
    public String getThreadGroupName() {
        return m_threadGroupName;
    }

    @Override
    public void setThreadGroupName(String threadGroupName) {
        m_threadGroupName = threadGroupName;
    }

    @Override
    public String getThreadId() {
        return m_threadId;
    }

    @Override
    public void setThreadId(String threadId) {
        m_threadId = threadId;
    }

    @Override
    public String getThreadName() {
        return m_threadName;
    }

    @Override
    public void setThreadName(String threadName) {
        m_threadName = threadName;
    }

    @Override
    public boolean isProcessLoss() {
        return m_processLoss;
    }

    @Override
    public void setProcessLoss(boolean loss) {
        m_processLoss = loss;
    }

    public void setDiscard(boolean discard) {
        m_discard = discard;
    }

    @Override
    public boolean isHitSample() {
        return m_hitSample;
    }

    @Override
    public void setHitSample(boolean hitSample) {
        m_hitSample = hitSample;
    }

    public void setDiscardPrivate(boolean discard) {
        m_discard = discard;
    }

}
