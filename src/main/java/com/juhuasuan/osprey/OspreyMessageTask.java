/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey;

import java.util.Iterator;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.juhuasuan.osprey.OspreyProcessor.OspreyPreProcessor;
import com.juhuasuan.osprey.store.BytesKey;
import com.juhuasuan.osprey.store.UniqId;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-15
 * @version 1.0
 */
public class OspreyMessageTask implements Runnable {
    private static final Logger logger = Logger.getLogger(OspreyMessageTask.class);

    private final OspreyManager ospreyManager;
    private final ThreadPoolExecutor asynSendMessageWorkTP;
    private final int threshold;

    private volatile int corePoolSize;
    private volatile int maxPoolSize;
    private volatile long keepAliveTime;
    private volatile int maxQueueSize;
    private AtomicLong lostCount = new AtomicLong(0L);

    private volatile boolean run = true;

    private volatile boolean suspend = false;

    private volatile int lostWaitWeight = 1;

    public OspreyMessageTask(OspreyManager ospreyManager) {
        this(ospreyManager, 5, 10, 1000, 100, 100);
    }

    public OspreyMessageTask(OspreyManager ospreyManager, int corePoolSize, int maxPoolSize, long keepAliveTime, int maxQueueSize, int threhold) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.maxQueueSize = maxQueueSize;
        this.threshold = threhold;
        if (null == ospreyManager) {
            throw new NullPointerException("null == ospreyManager");
        }
        this.ospreyManager = ospreyManager;
        this.asynSendMessageWorkTP = new ThreadPoolExecutor(this.corePoolSize, this.maxPoolSize, this.keepAliveTime, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());
    }

    public int getWaitCount() {
        int count = ospreyManager.storeSize();
        if (count > maxPoolSize) {
            count = maxPoolSize;
        }
        if (lostCount.get() > count / 2) {
            logger.error("More than half of messages transfer error. Wait for " + (10 * lostWaitWeight) + " and then we will try again. Don't warry, your message is safe!");
            sleep(10000 * lostWaitWeight);
            lostWaitWeight++;
            if (lostWaitWeight > 30) {
                lostWaitWeight = 30;
            }
            lostCount.set(0L);
        } else {
            lostWaitWeight = 1;
        }
        return count;
    }

    public boolean isSuspend() {
        return suspend;
    }

    public void setSuspend(boolean suspend) {
        this.suspend = suspend;
    }

    public void run() {
        logger.info("Osprey message task start...");
        Thread.currentThread().setName("OspreyMessageTask");
        boolean isFirst = true;
        while (run) {
            boolean bSend = false;

            if (suspend) {
                if (isFirst) {
                    logger.warn(">>>> Suspend for the first time.");
                    isFirst = false;
                }
                sleep(5000);
                continue;
            }

            if (null == ospreyManager.getStore()) {
                sleep(1000);
                continue;
            }
            int count = getWaitCount();
            try {
                Iterator<byte[]> iterator = ospreyManager.getIterator();
                while (iterator.hasNext()) {
                    byte[] messageId = iterator.next();
                    if (count <= 0) {
                        count = getWaitCount();
                    }

                    BytesKey messageIdKey = new BytesKey(messageId);
                    if (!ProcessRegister.getInstance().register(messageIdKey)) {
                        continue;
                    }

                    MessageInStore messageInStore = ospreyManager.getMessageInStore4j(messageId);
                    if (null == messageInStore) {
                        ospreyManager.removeMessage(messageId);
                        ProcessRegister.getInstance().unregister(messageIdKey);
                        bSend = true;
                        break;
                    }

                    if (messageInStore.isEnabledSend()) {
                        Message sendMessage = messageInStore.getMessage();
                        if (sendMessage != null) {
                            try {
                                asynSendMessageWorkTP.execute(new ReliableAsynSendMessageTask(ospreyManager, sendMessage, messageIdKey, this));
                            } catch (Exception e) {
                                logger.warn("Execute message error.", e);
                                ProcessRegister.getInstance().unregister(messageIdKey);
                                sleep(1000);
                            } finally {
                                bSend = true;
                                count--;
                            }
                        }
                    } else {
                        try {
                            if (System.currentTimeMillis() - messageInStore.getCreateTime() > 10000) {
                                Message sendMessage = messageInStore.getMessage();
                                final MessageStatus status = new MessageStatus();
                                new CheckListener() {
                                    public void receiveCheckMessage(Message message, MessageStatus status) {
                                        // simple do nothing
                                    }
                                }.receiveCheckMessage(sendMessage, status);
                                try {
                                    if (status.isRollbackOnly()) {
                                        logger.warn("Message MsgId:[" + messageId + "]");
                                        ospreyManager.removeMessage(messageId);
                                    } else {
                                        ospreyManager.commitMessage(sendMessage, new Result());
                                    }
                                } catch (Exception e) {
                                    logger.error("call receive check message error", e);
                                }
                            }
                        } finally {
                            ProcessRegister.getInstance().unregister(messageIdKey);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("Store error.", t);
            }

            if (!bSend && run) {
                sleep(1000);
            }
        }
        logger.error("Osprey task shutdown, message size : " + ospreyManager.storeSize());
        asynSendMessageWorkTP.shutdown();
    }

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            logger.error("Thread.sleep() interupted.", e);
            Thread.currentThread().interrupt();
        }
    }

    public boolean isRun() {
        return run;
    }

    public void setRun(boolean run) {
        this.run = run;
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public void addLostCount() {
        lostCount.incrementAndGet();
    }

    public void clearLostCount() {
        lostCount.set(0L);
    }

    private class ReliableAsynSendMessageTask implements Runnable {
        private final Message message;
        private final OspreyManager ospreyManager;
        private final OspreyMessageTask ospreyMessageTask;
        private final BytesKey messageIdKey;
        private final ProcessorUtil processorUtil;

        public ReliableAsynSendMessageTask(OspreyManager ospreyManager, Message message, BytesKey messageIdKey, OspreyMessageTask ospreyMessageTask) {
            this.ospreyManager = ospreyManager;
            this.message = message;
            this.ospreyMessageTask = ospreyMessageTask;
            this.messageIdKey = messageIdKey;
            this.processorUtil = ospreyManager.getProcessorUtil();
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public void run() {
            try {
                byte[] messageId = message.getMessageId();
                OspreyProcessor processor = processorUtil.findProcessor(message.getClass());
                if (processor instanceof OspreyPreProcessor) {
                    ((OspreyPreProcessor) processor).beforeProcess(message);
                }
                Result sendResult = processor.process(message);
                if (sendResult.isSuccess()) {
                    ospreyManager.removeMessage(messageId);
                } else {
                    message.incrementLostCount();

                    if (message.getLostCount() > threshold) {
                        logger.warn("Exceed max error threshold, remove it. Message MsgId:[" + messageId + "]");
                        ospreyManager.removeMessage(messageId);
                    }

                    ospreyMessageTask.addLostCount();
                    logger.error("Process message faild. MessageID = " + UniqId.getInstance().bytes2string(messageId) + " Error message:" + sendResult.getErrorMessage() + " Exception : " + sendResult.getRuntimeException());
                }
            } catch (Throwable t) {
                ospreyMessageTask.addLostCount();
                logger.error("Process message error: ", t);
            } finally {
                ProcessRegister.getInstance().unregister(messageIdKey);
            }
        }
    }
}
