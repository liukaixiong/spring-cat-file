package org.unidal.cat.message.config;

import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class ServerConfigManager {

    public static final String DUMP_DIR = "";
    private Map<String, String> hdfsProperties;
    private int messageDumpThreads = 5;
    private int messageProcessorThreads = 20;
    private boolean stroargeNioEnable = true;

    public int getHdfsUploadThreadsCount() {
        return 5;
    }

    public String getHdfsLocalBaseDir(String dump) {
        return null;
    }

    public int getLogViewStroageTime() {
        return 100;
    }

    public boolean isHdfsOn() {
        return false;
    }

    public String getHdfsBaseDir(String dump) {
        return null;
    }

    public String getHdfsServerUri(String dump) {
        return null;
    }

    public Map<String, String> getHdfsProperties() {
        return hdfsProperties;
    }

    public int getMessageDumpThreads() {
        return messageDumpThreads;
    }

    public int getMessageProcessorThreads() {
        return messageProcessorThreads;
    }

    public boolean getStroargeNioEnable() {
        return stroargeNioEnable;
    }

    public boolean isHarMode() {
        return false;
    }

    public String getHarfsServerUri(String id) {
        return null;
    }

    public String getHarfsBaseDir(String id) {
        return null;
    }
}
