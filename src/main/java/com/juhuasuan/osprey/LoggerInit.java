package com.juhuasuan.osprey;

import java.io.File;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-16 下午2:35:57
 * @version 1.0
 */
public class LoggerInit {

    static final String LOGGER_NAME = "com.juhuasuan.osprey";

    static public final Log LOGGER;

    static {
        // 告诉commons-logging具体log实现
        LogFactory.getFactory().setAttribute(LogFactoryImpl.LOG_PROPERTY, Log4JLogger.class.getName());
        LOGGER = LogFactory.getLog(LOGGER_NAME);

        try { // 不能因为该类初始化失败导致其引用类初始化失败
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static private void init() {
        ClassLoader oldTCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(LoggerInit.class.getClassLoader());

        try {
            DOMConfigurator.configure(LoggerInit.class.getClassLoader().getResource("osprey-log4j.xml"));
            Logger log4jLogger = Logger.getLogger(LOGGER_NAME);

            /*
             * 找到上层应用在Root Logger上设置的FileAppender，以及通信层配置的FileAppender。
             * 目的是为了让通信层的日志与上层应用的日志输出到同一个目录。
             */
            FileAppender bizFileAppender = getFileAppender(Logger.getRootLogger());
            if (null == bizFileAppender) {
                LOGGER.warn("上层业务层没有在ROOT LOGGER上设置FileAppender!!!");
                return;
            }
            FileAppender fileAppender = getFileAppender(log4jLogger);

            // 并创建异步Appender来替代FileAppender。
            String bizLogDir = new File(bizFileAppender.getFile()).getParent();
            String logFile = new File(bizLogDir, "osprey.log").getAbsolutePath();
            fileAppender.setFile(logFile);
            fileAppender.activateOptions(); // 很重要，否则原有日志内容会被清空
            AsyncAppender asynAppender = new AsyncAppender();
            asynAppender.addAppender(fileAppender);
            log4jLogger.addAppender(asynAppender);
            log4jLogger.removeAppender(fileAppender);
            LOGGER.warn("成功为OSPREY LOGGER添加Appender. 输出路径:" + logFile);
        } finally {
            Thread.currentThread().setContextClassLoader(oldTCL);
        }
    }

    @SuppressWarnings("unchecked")
    static private FileAppender getFileAppender(Logger logger) {
        Enumeration<Appender> ppp = logger.getAllAppenders();
        return searchFileAP(ppp);
    }

    @SuppressWarnings("unchecked")
    static FileAppender searchFileAP(Enumeration<Appender> ppp) {
        FileAppender fp = null;

        while (null == fp && ppp.hasMoreElements()) {
            Appender ap = ppp.nextElement();
            if (ap instanceof FileAppender) {
                fp = (FileAppender) ap;
            } else if (ap instanceof AsyncAppender) {
                ppp = ((AsyncAppender) ap).getAllAppenders();
                fp = searchFileAP(ppp);
            }
        }
        return fp;
    }

}
