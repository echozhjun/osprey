/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
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
 * @since 2012-3-16
 * @version 1.0
 */
public class LoggerInit {

    static final String LOGGER_NAME = "com.juhuasuan.osprey";

    static public final Log LOGGER;

    static {
        LogFactory.getFactory().setAttribute(LogFactoryImpl.LOG_PROPERTY, Log4JLogger.class.getName());
        LOGGER = LogFactory.getLog(LOGGER_NAME);

        try {
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

            FileAppender bizFileAppender = getFileAppender(Logger.getRootLogger());
            if (null == bizFileAppender) {
                LOGGER.warn("ROOT LOGGER dosn't contain any FileAppender!!!");
                return;
            }
            FileAppender fileAppender = getFileAppender(log4jLogger);

            String bizLogDir = new File(bizFileAppender.getFile()).getParent();
            String logFile = new File(bizLogDir, "osprey.log").getAbsolutePath();
            fileAppender.setFile(logFile);
            fileAppender.activateOptions();
            AsyncAppender asynAppender = new AsyncAppender();
            asynAppender.addAppender(fileAppender);
            log4jLogger.addAppender(asynAppender);
            log4jLogger.removeAppender(fileAppender);
            LOGGER.warn("OSPREY LOGGER added Appender. Append file:" + logFile);
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
