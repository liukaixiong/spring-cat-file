package com.cat.file.message.config;

import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class ServerConfigManager {

    public static final String DUMP_DIR = "";
    private Map<String, String> hdfsProperties;

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
