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
package com.cat.file.message.config.props;

import com.cat.file.message.bean.Initializable;
import com.cat.file.message.bean.InitializationException;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.File;


@ConfigurationProperties(prefix = "spring.cat.file")
public class StorageProperties implements Initializable {

    /**
     * 文件存储目录位置
     */
    private String baseDataDir = "\\data\\appdatas\\cat\\bucket\\";

    /**
     * 是否本地存储
     */
    private boolean isLocalModel = true;

    /**
     * 消息dump开启线程
     */
    private int messageDumpThreads = 5;

    /**
     * 消息处理开启线程数
     */
    private int messageProcessorThreads = 20;

    /**
     * 是否开启Nio存储
     */
    private boolean storageNioEnable = true;

    @Override
    public void initialize() throws InitializationException {
        // setBaseDataDir(new File(LogUtils.getCatHome(), "bucket"));
        System.out.println("暂时还没打算用");
    }


    public int getMessageDumpThreads() {
        return messageDumpThreads;
    }

    public void setMessageDumpThreads(int messageDumpThreads) {
        this.messageDumpThreads = messageDumpThreads;
    }

    public int getMessageProcessorThreads() {
        return messageProcessorThreads;
    }

    public void setMessageProcessorThreads(int messageProcessorThreads) {
        this.messageProcessorThreads = messageProcessorThreads;
    }

    public boolean isStorageNioEnable() {
        return storageNioEnable;
    }

    public void setStorageNioEnable(boolean storageNioEnable) {
        this.storageNioEnable = storageNioEnable;
    }

    public String getBaseDataDir() {
        return baseDataDir;
    }

    public void setBaseDataDir(String baseDataDir) {
        this.baseDataDir = baseDataDir;
    }

    public boolean isLocalModel() {
        return isLocalModel;
    }

    public void setLocalModel(boolean localModel) {
        isLocalModel = localModel;
    }

    public void setBaseDataDir(File baseDataDir) {
        this.baseDataDir = baseDataDir.getAbsolutePath() + '/';
    }

}
