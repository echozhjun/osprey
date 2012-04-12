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
 * @since 2012-3-15 ����5:13:37
 * @version 1.0
 */
public class OspreyMessageTask implements Runnable {
    private static final Logger logger = Logger.getLogger(OspreyMessageTask.class);

    private final OspreyManager ospreyManager;
    private final ThreadPoolExecutor asynSendMessageWorkTP;
    // �������������������Ϣ
    private final int threshold;

    private volatile int corePoolSize;
    private volatile int maxPoolSize;
    private volatile long keepAliveTime;
    private volatile int maxQueueSize;
    private AtomicLong lostCount = new AtomicLong(0L);

    private volatile boolean run = true;

    /**
     * ��ǰ�Ƿ�����ͣ�ɿ��첽����״̬
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
        // ������Ҫע�⣺һ���ϴη���ʧ�ܵĴ��ڷ�����Ϣ������һ��ʱ��
        // ������Ϊ������Ϣ�����á�����������£�˯��1���ӣ��ٳ��Է���
        if (lostCount.get() > count / 2) {
            logger.error("�ڿɿ��첽�����У��ϸ������첽������Ϣ���Ͳ���ȥ������" + (10 * lostWaitWeight) + "�룬Ȼ���ٴγ��Կɿ��첽������Ϣ��δ���͵���Ϣ�洢�ڱ���Ӳ�̣���ͻ����ģ���Ϣ�ǰ�ȫ��");
            sleep(10000 * lostWaitWeight);
            lostWaitWeight++;
            if (lostWaitWeight > 30) {
                lostWaitWeight = 30;// ��ȴ�5����
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
        logger.info("����ɿ��첽������Ϣ����");
        Thread.currentThread().setName("ReliableAsynTraverseMessageTask");
        boolean isFirst = true;
        while (run) {
            boolean bSend = false;

            // �û���������ͣ�ɿ��첽����
            if (suspend) {
                if (isFirst) {
                    logger.warn(">>>>>�ɿ��첽������Ϣ���̱��û���ͣ");
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
                                logger.warn("��ϢͶ�ݹ���", e);
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
                                        logger.warn("������Ϣ������û����ɣ�ɾ����Message MsgId:[" + messageId + "]");
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
                logger.error("����Store4j����", t);
            }

            if (!bSend && run) {
                sleep(1000);
            }
        }
        logger.error("�ɿ��첽����ʱ��Store4j����Ϣ��������" + ospreyManager.storeSize());
        asynSendMessageWorkTP.shutdown();
    }

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            logger.error("Thread.sleep()����", e);
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
                        logger.warn("�����趨�Ĵ�����ֵ��������Message MsgId:[" + messageId + "]");
                        ospreyManager.removeMessage(messageId);
                    }

                    ospreyMessageTask.addLostCount();
                    logger.error("�ɿ��첽��[ͬ��������Ϣ]�׶δ���MessageID��" + UniqId.getInstance().bytes2string(messageId) + "������ԭ��" + sendResult.getErrorMessage() + "���쳣��" + sendResult.getRuntimeException());
                }
            } catch (Throwable t) {
                ospreyMessageTask.addLostCount();
                logger.error("�ɿ��첽��[ͬ��������Ϣ]�׶δ���: ", t);
            } finally {
                ProcessRegister.getInstance().unregister(messageIdKey);
            }
        }
    }
}
