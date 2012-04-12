package com.juhuasuan.osprey;

import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-16 ����2:29:32
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
            LOGGER.warn("�߳�[" + Thread.currentThread().getName() + "]�ڵ���DIYExecutorʱ��ִ�е�����ΪNULL.");
        }
    }

}
