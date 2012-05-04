/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey;

import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-16
 * @version 1.0
 */
public class DIYExecutor implements Executor {

    private final Log LOGGER = LoggerInit.LOGGER;

    static private final DIYExecutor instance = new DIYExecutor();

    static public DIYExecutor getInstance() {
        return instance;
    }

    public void execute(Runnable task) {
        if (null != task) {
            task.run();
        } else {
            LOGGER.warn(Thread.currentThread().getName() + " DIYExecutor task is null.");
        }
    }

}
