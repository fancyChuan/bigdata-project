package cn.fancychuan.logs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogQuickstartApp {
    public static void main(String[] args) {
        demo();
    }


    public static void demo() {
        Logger logger = LoggerFactory.getLogger("SampleLogger");
        logger.info("hello slf4j ~ ");
        logger.error("这是error");

        Logger logger1 = LoggerFactory.getLogger(LoggerFactory.class);
        logger1.warn("这是新的一个日志");
    }
}
