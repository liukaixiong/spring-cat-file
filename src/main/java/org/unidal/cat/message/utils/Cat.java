package org.unidal.cat.message.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cat {
    private static Logger logger = LoggerFactory.getLogger(Cat.class);
    private static String catHome;

    public static void logError(Throwable e) {
        logger.error("error",e);
    }

    public static void logError(String log, Throwable e) {
        logger.error(log,e);
    }

    public static String getCatHome() {
        return catHome;
    }

    public static void logEvent(String abnormalBlock, String domain) {
        logger.info("Cat监控日志: {} || {}",abnormalBlock,domain);
    }
}
