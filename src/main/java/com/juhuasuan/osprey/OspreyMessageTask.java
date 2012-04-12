package com.juhuasuan.osprey;

import java.util.Iterator;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.juhuasuan.osprey.OspreyProcessor.OspreyPreProcessor;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.UniqId;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-15 下午5:13:37
 * @version 1.0
 */
public class OspreyMessageTask implements Runnable {
    private static final Logger logger = Logger.getLogger(OspreyMessageTask.class);

    private final OspreyManager ospreyManager;
    private final ThreadPoolExecutor asynSendMessageWorkTP;
    // 超过这个数量则抛弃消息
    private final int threshold;

    private volatile int corePoolSize;
    private volatile int maxPoolSize;
    private volatile long keepAliveTime;
    private volatile int maxQueueSize;
    private AtomicLong lostCount = new AtomicLong(0L);

    private volatile boolean run = true;

    /**
     * 当前是否处于暂停可靠异步任务状态
     */
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
        // 这里需要注意：一旦上次发送失败的大于发送消息数量的一半时，
        // 我们认为发送消息不可用。在这种情况下，睡眠1分钟，再尝试发送
        if (lostCount.get() > count / 2) {
            logger.error("在可靠异步发送中，上个周期异步发送消息发送不出去，休眠" + (10 * lostWaitWeight) + "秒，然后再次尝试可靠异步发送消息，未发送的消息存储在本地硬盘，请客户放心，消息是安全的");
            sleep(10000 * lostWaitWeight);
            lostWaitWeight++;
            if (lostWaitWeight > 30) {
                lostWaitWeight = 30;// 最长等待5分钟
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
        logger.info("进入可靠异步发送消息流程");
        Thread.currentThread().setName("ReliableAsynTraverseMessageTask");
        boolean isFirst = true;
        while (run) {
            boolean bSend = false;

            // 用户设置了暂停可靠异步发送
            if (suspend) {
                if (isFirst) {
                    logger.warn(">>>>>可靠异步发送消息流程被用户暂停");
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

                    MessageInStore4j messageInStore4j = ospreyManager.getMessageInStore4j(messageId);
                    if (null == messageInStore4j) {
                        ospreyManager.removeMessage(messageId);
                        ProcessRegister.getInstance().unregister(messageIdKey);
                        bSend = true;
                        break;
                    }

                    if (messageInStore4j.isEnabledSend()) {
                        Message sendMessage = messageInStore4j.getMessage();
                        if (sendMessage != null) {
                            try {
                                asynSendMessageWorkTP.execute(new ReliableAsynSendMessageTask(ospreyManager, sendMessage, messageIdKey, this));
                            } catch (Exception e) {
                                logger.warn("消息投递过快", e);
                                ProcessRegister.getInstance().unregister(messageIdKey);
                                sleep(1000);
                            } finally {
                                bSend = true;
                                count--;
                            }
                        }
                    } else {
                        try {
                            if (System.currentTimeMillis() - messageInStore4j.getCreateTime() > 10000) {
                                Message sendMessage = messageInStore4j.getMessage();
                                final MessageStatus status = new MessageStatus();
                                new CheckListener() {
                                    public void receiveCheckMessage(Message message, MessageStatus status) {
                                        // simple do nothing
                                    }
                                }.receiveCheckMessage(sendMessage, status);
                                try {
                                    if (status.isRollbackOnly()) {
                                        logger.warn("事务消息的事务没有完成，删除该Message MsgId:[" + messageId + "]");
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
                logger.error("遍历Store4j出错", t);
            }

            if (!bSend && run) {
                sleep(1000);
            }
        }
        logger.error("可靠异步结束时，Store4j中消息的数量：" + ospreyManager.storeSize());
        asynSendMessageWorkTP.shutdown();
    }

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            logger.error("Thread.sleep()出错", e);
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
                        logger.warn("超过设定的错误阈值，抛弃该Message MsgId:[" + messageId + "]");
                        ospreyManager.removeMessage(messageId);
                    }

                    ospreyMessageTask.addLostCount();
                    logger.error("可靠异步的[同步发送消息]阶段错误，MessageID：" + UniqId.getInstance().bytes2string(messageId) + "，错误原因：" + sendResult.getErrorMessage() + "，异常：" + sendResult.getRuntimeException());
                }
            } catch (Throwable t) {
                ospreyMessageTask.addLostCount();
                logger.error("可靠异步的[同步发送消息]阶段错误: ", t);
            } finally {
                ProcessRegister.getInstance().unregister(messageIdKey);
            }
        }
    }
}
