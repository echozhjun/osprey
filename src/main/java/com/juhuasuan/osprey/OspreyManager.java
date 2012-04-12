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
 * @since 2012-3-15 下午4:05:40
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

    // 已经发送的commit消息总数量
    private AtomicInteger messageTotalCount = new AtomicInteger(0);

    // store4j中剩余commit消息
    private AtomicInteger remainCommitMessageCount = new AtomicInteger(0);

    private long ospreyKeepAliveTime = 60L; // 60 second。
    // 在设置单位时，为TimeUnit.SECONDS
    private int ospreyCorePoolSize = 10;
    private int ospreyMaxPoolSize = 20;
    private int ospreyMaxQueueSize = 10000;
    private OspreyMessageTask ospreyMessageTask;

    private int maxStoreSize = Integer.MAX_VALUE;

    private volatile boolean isInit = false;

    // 用户在初始化之前调用了停止可靠异步发送消息
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
            logger.error("创建可靠异步目录不成功");
            throw new RuntimeException("创建可靠异步目录不成功");
        }
        try {
            store = new JournalStore(localMessagePath, "osprey_" + storeName, isForceToDisk, true, true);
        } catch (IOException e) {
            logger.error("初始化Store4j错误，路径为：" + localMessagePath, e);
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

            logger.error("初始化消息计数器失败! 本次数据只记录自启动以来的消息条数!", e);
        } finally {
            // 初始化本地消息计数器
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
            logger.error("关闭Store4j文件失败", e);
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
                sendResult.setErrorMessage("Store4j中存储的消息量超过阀值");
                RuntimeException re = new RuntimeException("Store4j中存储的消息量超过阀值");
                sendResult.setRuntimeException(re);
                sendResult.setSendResultType(ResultType.ERROR);
                logger.error("Store4j中存储的消息量超过阀值");
                return sendResult;
            }
            try {
                MessageInStore4j messageInStore4j = new MessageInStore4j(message, committed);
                store.add(message.getMessageId(), serializer.serialize(messageInStore4j));
                if (null != cache) {
                    cache.put(message.getMessageId(), messageInStore4j);
                }

                // 添加消息是commit消息则计数器增加
                if (committed) {
                    messageTotalCount.incrementAndGet();
                    remainCommitMessageCount.incrementAndGet();
                }

            } catch (IOException e) {
                sendResult.setSuccess(false);
                sendResult.setSendResultType(ResultType.EXCEPTION);
                sendResult.setErrorMessage("添加Message到Store4j错误");
                RuntimeException re = new RuntimeException("消息存储在本地失败", e);
                sendResult.setRuntimeException(re);
                logger.error("添加Message到Store4j错误，MessageID：" + sendResult.getMessageId(), e);
            }
            return sendResult;
        } else {
            sendResult.setSuccess(false);
            sendResult.setErrorMessage("添加Message到Store4j错误，Store4j初始化错误");
            RuntimeException re = new RuntimeException("Store4j初始化错误");
            sendResult.setRuntimeException(re);
            sendResult.setSendResultType(ResultType.ERROR);
            logger.error("添加Message到Store4j错误，Store4j初始化错误");
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

                // commit成功计数器增加
                messageTotalCount.incrementAndGet();
                remainCommitMessageCount.incrementAndGet();
            } catch (IOException e) {
                sendResult.setSuccess(false);
                sendResult.setErrorMessage("更新Message到Store4j错误");
                RuntimeException re = new RuntimeException("更新在本地的Message失败", e);
                sendResult.setRuntimeException(re);
                logger.error("更新Message到Store4j错误，MessageID：" + sendResult.getMessageId(), e);
            }
            return sendResult;
        } else {
            sendResult.setSuccess(false);
            sendResult.setErrorMessage("更新Message到Store4j错误，Store4j初始化错误");
            RuntimeException re = new RuntimeException("Store4j初始化错误");
            sendResult.setRuntimeException(re);
            logger.error("更新Message到Store4j错误，Store4j初始化错误");
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
                sendResult.setErrorMessage("回滚消息时，删除Store4j中的Message错误");
                RuntimeException re = new RuntimeException("回滚消息时，删除存储在本地的Message失败", e);
                sendResult.setRuntimeException(re);
                logger.error("回滚消息时，删除Store4j中的Message错误，MessageID：" + sendResult.getMessageId(), e);
            }
            return sendResult;
        } else {
            sendResult.setSuccess(false);
            sendResult.setErrorMessage("回滚Store4j中消息错误，Store4j初始化错误");
            RuntimeException re = new RuntimeException("Store4j初始化错误");
            sendResult.setRuntimeException(re);
            logger.error("回滚Store4j中消息错误，Store4j初始化错误");
            return sendResult;
        }
    }

    /**
     * 可靠异步发送成功才去调用该方法删除消息
     * 
     * @param msgId
     */
    public void removeMessage(byte[] msgId) {
        if (init()) {
            try {
                if (!store.remove(msgId)) {
                    logger.error("无法删除数据：" + msgId);
                }
                if (null != cache) {
                    cache.remove(msgId);
                }

                // 可靠异步发送到NS成功,计数器递减
                this.remainCommitMessageCount.decrementAndGet();
            } catch (IOException e) {
                logger.error("删除消息时，删除Store4j中的Message错误，MessageID：" + UniqId.getInstance().bytes2string(msgId), e);
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
                    logger.warn("获取一个空的messageId: " + UniqId.getInstance().bytes2string(msgId));
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
                    logger.warn("反序列化MessageInStore4j有误  MsgId:[" + msgId + "]", e);
                    return null;
                }
                if (null == messageInStore4j) {
                    logger.warn("反序列化MessageInStore4j有误，null == messageInStore4j  MsgId:[" + msgId + "]");
                    return null;
                }
                if (null != cache) {
                    cache.put(msgId, messageInStore4j);
                }
                return messageInStore4j;
            } catch (Throwable t) {
                logger.error("获取Store4j中消息错误", t);
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
