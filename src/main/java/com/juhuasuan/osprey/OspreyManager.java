package com.juhuasuan.osprey;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.juhuasuan.osprey.cache.LRUSoftMessageCache;
import com.taobao.common.store.Store;
import com.taobao.common.store.journal.JournalStore;
import com.taobao.common.store.util.UniqId;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-15 ����4:05:40
 * @version 1.0
 */
public class OspreyManager {
    private static final Logger logger = Logger.getLogger(OspreyManager.class);

    private ExecutorService ospreySendMessageWorkTP;

    private Store store;

    private String localMessagePath;
    private String storeName;
    private boolean isForceToDisk;
    private int threhold;

    // �Ѿ����͵�commit��Ϣ������
    private AtomicInteger messageTotalCount = new AtomicInteger(0);

    // store4j��ʣ��commit��Ϣ
    private AtomicInteger remainCommitMessageCount = new AtomicInteger(0);

    private long ospreyKeepAliveTime = 60L; // 60 second��
    // �����õ�λʱ��ΪTimeUnit.SECONDS
    private int ospreyCorePoolSize = 10;
    private int ospreyMaxPoolSize = 20;
    private int ospreyMaxQueueSize = 10000;
    private OspreyMessageTask ospreyMessageTask;

    private int maxStoreSize = Integer.MAX_VALUE;

    private volatile boolean isInit = false;

    // �û��ڳ�ʼ��֮ǰ������ֹͣ�ɿ��첽������Ϣ
    private volatile boolean isSuspendBeforeInit = false;

    private final HessianSerializer serializer = new HessianSerializer();

    private LRUSoftMessageCache cache = null;

    private long maxStoreFileCount = Long.MAX_VALUE;

    private ProcessorUtil processorUtil = new ProcessorUtil();

    public OspreyManager(String storeName) {
        this(System.getProperty("user.home") + "/osprey/", storeName, true);
    }

    public OspreyManager(String localMessagePath, String storeName, boolean isForceToDisk) {
        this(localMessagePath, storeName, 100, isForceToDisk, 1000, 2000);
    }

    public OspreyManager(String localMessagePath, String storeName, int threhold, boolean isForceToDisk, int lowWaterMark, int highWaterMark) {
        this.localMessagePath = localMessagePath;
        this.storeName = storeName;
        this.isForceToDisk = isForceToDisk;
        this.threhold = threhold;
        this.cache = new LRUSoftMessageCache(lowWaterMark, highWaterMark);
    }

    public synchronized boolean init() {
        if (isInit) {
            return isInit;
        }
        try {
            File dir = new File(localMessagePath);
            if (!dir.exists()) {
                dir.mkdirs();
            }
        } catch (Exception e) {
            logger.error("�����ɿ��첽Ŀ¼���ɹ�");
            throw new RuntimeException("�����ɿ��첽Ŀ¼���ɹ�");
        }
        try {
            store = new JournalStore(localMessagePath, "osprey_" + storeName, isForceToDisk, true, true);
        } catch (IOException e) {
            logger.error("��ʼ��Store4j����·��Ϊ��" + localMessagePath, e);
            return isInit;
        }
        ospreyMessageTask = new OspreyMessageTask(this, ospreyCorePoolSize, ospreyMaxPoolSize, ospreyKeepAliveTime, ospreyMaxQueueSize, threhold);
        store.setMaxFileCount(this.maxStoreFileCount);
        ospreySendMessageWorkTP = Executors.newSingleThreadExecutor();
        isInit = true;

        initMessageCount();

        ospreySendMessageWorkTP.execute(ospreyMessageTask);

        return isInit;
    }

    private void initMessageCount() {
        int initCount = 0;
        Iterator<byte[]> messageId = null;
        try {
            messageId = this.getIterator();

            while (messageId.hasNext()) {
                MessageInStore4j msg = this.getMessageInStore4j(messageId.next());
                if (null != msg && msg.isEnabledSend()) {
                    initCount++;
                }
            }
        } catch (IOException e) {

            logger.error("��ʼ����Ϣ������ʧ��! ��������ֻ��¼��������������Ϣ����!", e);
        } finally {
            // ��ʼ��������Ϣ������
            this.remainCommitMessageCount.set(initCount);
            this.messageTotalCount.set(initCount);
        }
    }

    public boolean isInit() {
        return isInit;
    }

    public synchronized void close() {
        if (!isInit) {
            return;
        }

        isInit = false;
        ospreyMessageTask.setRun(false);
        ospreySendMessageWorkTP.shutdown();
        ospreyMessageTask = null;
        ospreySendMessageWorkTP = null;

        try {
            if (null != store) {
                store.close();
            }
        } catch (IOException e) {
            logger.error("�ر�Store4j�ļ�ʧ��", e);
        }
        store = null;
    }

    public Result addMessage(Message message, boolean committed) {
        Result sendResult = new Result();
        if (init()) {
            sendResult.setMessageId(UniqId.getInstance().bytes2string(message.getMessageId()));
            sendResult.setSuccess(true);
            sendResult.setSendResultType(ResultType.SUCCESS);
            if (store.size() >= maxStoreSize) {
                sendResult.setSuccess(false);
                sendResult.setErrorMessage("Store4j�д洢����Ϣ��������ֵ");
                RuntimeException re = new RuntimeException("Store4j�д洢����Ϣ��������ֵ");
                sendResult.setRuntimeException(re);
                sendResult.setSendResultType(ResultType.ERROR);
                logger.error("Store4j�д洢����Ϣ��������ֵ");
                return sendResult;
            }
            try {
                MessageInStore4j messageInStore4j = new MessageInStore4j(message, committed);
                store.add(message.getMessageId(), serializer.serialize(messageInStore4j));
                if (null != cache) {
                    cache.put(message.getMessageId(), messageInStore4j);
                }

                // �����Ϣ��commit��Ϣ�����������
                if (committed) {
                    messageTotalCount.incrementAndGet();
                    remainCommitMessageCount.incrementAndGet();
                }

            } catch (IOException e) {
                sendResult.setSuccess(false);
                sendResult.setSendResultType(ResultType.EXCEPTION);
                sendResult.setErrorMessage("���Message��Store4j����");
                RuntimeException re = new RuntimeException("��Ϣ�洢�ڱ���ʧ��", e);
                sendResult.setRuntimeException(re);
                logger.error("���Message��Store4j����MessageID��" + sendResult.getMessageId(), e);
            }
            return sendResult;
        } else {
            sendResult.setSuccess(false);
            sendResult.setErrorMessage("���Message��Store4j����Store4j��ʼ������");
            RuntimeException re = new RuntimeException("Store4j��ʼ������");
            sendResult.setRuntimeException(re);
            sendResult.setSendResultType(ResultType.ERROR);
            logger.error("���Message��Store4j����Store4j��ʼ������");
            return sendResult;
        }
    }

    public Result commitMessage(Message message, Result sendResult) {
        if (init()) {
            try {
                MessageInStore4j messageInStore4j = null;
                if (null != cache) {
                    messageInStore4j = cache.get(message.getMessageId());
                }
                if (null != messageInStore4j) {
                    messageInStore4j.setEnabledSend(true);
                } else {
                    messageInStore4j = new MessageInStore4j(message, true);
                    if (null != cache) {
                        cache.put(message.getMessageId(), messageInStore4j);
                    }
                }
                store.update(message.getMessageId(), serializer.serialize(messageInStore4j));

                // commit�ɹ�����������
                messageTotalCount.incrementAndGet();
                remainCommitMessageCount.incrementAndGet();
            } catch (IOException e) {
                sendResult.setSuccess(false);
                sendResult.setErrorMessage("����Message��Store4j����");
                RuntimeException re = new RuntimeException("�����ڱ��ص�Messageʧ��", e);
                sendResult.setRuntimeException(re);
                logger.error("����Message��Store4j����MessageID��" + sendResult.getMessageId(), e);
            }
            return sendResult;
        } else {
            sendResult.setSuccess(false);
            sendResult.setErrorMessage("����Message��Store4j����Store4j��ʼ������");
            RuntimeException re = new RuntimeException("Store4j��ʼ������");
            sendResult.setRuntimeException(re);
            logger.error("����Message��Store4j����Store4j��ʼ������");
            return sendResult;
        }
    }

    public Result rollbackMessage(Message message, Result sendResult) {

        if (init()) {
            try {
                store.remove(message.getMessageId());
                if (null != cache) {
                    cache.remove(message.getMessageId());
                }
            } catch (IOException e) {
                sendResult.setSuccess(false);
                sendResult.setErrorMessage("�ع���Ϣʱ��ɾ��Store4j�е�Message����");
                RuntimeException re = new RuntimeException("�ع���Ϣʱ��ɾ���洢�ڱ��ص�Messageʧ��", e);
                sendResult.setRuntimeException(re);
                logger.error("�ع���Ϣʱ��ɾ��Store4j�е�Message����MessageID��" + sendResult.getMessageId(), e);
            }
            return sendResult;
        } else {
            sendResult.setSuccess(false);
            sendResult.setErrorMessage("�ع�Store4j����Ϣ����Store4j��ʼ������");
            RuntimeException re = new RuntimeException("Store4j��ʼ������");
            sendResult.setRuntimeException(re);
            logger.error("�ع�Store4j����Ϣ����Store4j��ʼ������");
            return sendResult;
        }
    }

    /**
     * �ɿ��첽���ͳɹ���ȥ���ø÷���ɾ����Ϣ
     * 
     * @param msgId
     */
    public void removeMessage(byte[] msgId) {
        if (init()) {
            try {
                if (!store.remove(msgId)) {
                    logger.error("�޷�ɾ�����ݣ�" + msgId);
                }
                if (null != cache) {
                    cache.remove(msgId);
                }

                // �ɿ��첽���͵�NS�ɹ�,�������ݼ�
                this.remainCommitMessageCount.decrementAndGet();
            } catch (IOException e) {
                logger.error("ɾ����Ϣʱ��ɾ��Store4j�е�Message����MessageID��" + UniqId.getInstance().bytes2string(msgId), e);
            }
        }
    }

    public MessageInStore4j getMessageInStore4j(byte[] msgId) {
        if (init()) {

            MessageInStore4j messageInStore4j = null;
            if (null != cache) {
                messageInStore4j = cache.get(msgId);
            }
            if (null != messageInStore4j) {
                return messageInStore4j;
            }
            try {

                byte[] objectBytes = store.get(msgId);
                if (null == objectBytes) {
                    logger.warn("��ȡһ���յ�messageId: " + UniqId.getInstance().bytes2string(msgId));
                    return null;
                }
                try {
                    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                    try {
                        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                        messageInStore4j = (MessageInStore4j) serializer.deserialize(objectBytes);
                    } finally {
                        Thread.currentThread().setContextClassLoader(contextClassLoader);
                    }
                } catch (Exception e) {
                    logger.warn("�����л�MessageInStore4j����  MsgId:[" + msgId + "]", e);
                    return null;
                }
                if (null == messageInStore4j) {
                    logger.warn("�����л�MessageInStore4j����null == messageInStore4j  MsgId:[" + msgId + "]");
                    return null;
                }
                if (null != cache) {
                    cache.put(msgId, messageInStore4j);
                }
                return messageInStore4j;
            } catch (Throwable t) {
                logger.error("��ȡStore4j����Ϣ����", t);
                return null;
            }
        } else {
            return null;
        }
    }

    public Iterator<byte[]> getIterator() throws IOException {
        if (init()) {
            return store.iterator();
        }
        return null;
    }

    public void suspendRaliableAsynTask() {
        if (isInit) {
            this.ospreyMessageTask.setSuspend(true);
        }
        this.isSuspendBeforeInit = true;
    }

    public void resumeReliableAsynTask() {
        if (isInit) {
            this.ospreyMessageTask.setSuspend(false);
        }
        this.isSuspendBeforeInit = false;
    }

    public boolean isSuspendRaliableAsynTask() {
        if (isInit) {
            return this.ospreyMessageTask.isSuspend();
        }

        return this.isSuspendBeforeInit;
    }

    public void registerProcessor(OspreyProcessor<?> processor) {
        processorUtil.registerProcessor(processor);
    }

    public OspreyProcessor<?> removeProcessor(Class<?> appRequestClazz) {
        return processorUtil.removeProcessor(appRequestClazz);
    }

    public Map<Class<?>, OspreyProcessor<?>> getProcessors() {
        return processorUtil.getProcessors();
    }

    public void updateProcessors(Map<Class<?>, OspreyProcessor<?>> newProcessors) {
        processorUtil.updateProcessors(newProcessors);
    }

    public int getMessageTotalCount() {
        return this.messageTotalCount.get();
    }

    public int getRemainCommitMessageCount() {
        return this.remainCommitMessageCount.get();
    }

    public int storeSize() {
        if (isInit) {
            return store.size();
        }
        return 0;
    }

    public Store getStore() {
        return store;
    }

    public void setStore(Store store) {
        this.store = store;
    }

    public ProcessorUtil getProcessorUtil() {
        return processorUtil;
    }

    public int getThrehold() {
        return threhold;
    }

    public void setThrehold(int threhold) {
        this.threhold = threhold;
    }

    public void setOspreyCorePoolSize(int ospreyCorePoolSize) {
        this.ospreyCorePoolSize = ospreyCorePoolSize;
    }

    public void setOspreyMaxPoolSize(int ospreyMaxPoolSize) {
        this.ospreyMaxPoolSize = ospreyMaxPoolSize;
    }

}
