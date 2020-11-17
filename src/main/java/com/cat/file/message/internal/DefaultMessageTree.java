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

import java.util.Date;

public class DefaultMessageTree extends AbstractMessageTree<String> {
    /**
     * 消息内容
     */
    private String message;

    public DefaultMessageTree(String domain, String ipAddress, Integer id, Date createTime, String message) {
        super(domain, ipAddress, id, createTime, message);
        this.message = message;
    }

    public DefaultMessageTree(String domain, Integer id, Date createTime, String message) {
        super(domain, id, createTime, message);
        this.message = message;
    }

    @Override
    public String getMessage() {
        return this.message;
    }

    @Override
    public void setMessage(String message) {
        this.message = message;
    }
}
