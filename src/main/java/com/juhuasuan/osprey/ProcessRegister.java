package com.juhuasuan.osprey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.taobao.common.store.util.BytesKey;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-16 ����10:20:26
 * @version 1.0
 */
public final class ProcessRegister {
    private static final Logger log = Logger.getLogger(ProcessRegister.class);

    private ConcurrentHashMap<BytesKey, Long> map = new ConcurrentHashMap<BytesKey, Long>();

    private long lastCheckTimestamp = System.currentTimeMillis();

    private final long processRegisterCechkInterval = 30 * 1000;
    private final long processRegisterTimeout = 20 * 1000;

    private ProcessRegister() {
    }

    public int getProcessingMessageCount() {
        return map.size();
    }

    static class SingletonHolder {
        static ProcessRegister instance = new ProcessRegister();
    }

    public static ProcessRegister getInstance() {
        return SingletonHolder.instance;
    }

    public final boolean isRegistered(BytesKey key) {
        return map.containsKey(key);
    }

    public void clear() {
        this.map.clear();
    }

    public final boolean isEmpty() {
        return map.isEmpty();
    }

    public final boolean register(BytesKey key) {
        long currentTimeMillis = System.currentTimeMillis();
        boolean result = map.putIfAbsent(key, currentTimeMillis) == null;
        if (currentTimeMillis - this.lastCheckTimestamp > processRegisterCechkInterval) {
            evict(currentTimeMillis);
            this.lastCheckTimestamp = currentTimeMillis;
        }
        return result;
    }

    public int evict() {
        return evict(System.currentTimeMillis());
    }

    public final int evict(long currentTimeMillis) {
        int count = 0;
        for (Map.Entry<BytesKey, Long> entry : this.map.entrySet()) {
            // ����12��Сʱδ�Ƴ�������ǿ���Ƴ�����ֹ�ڴ�й¶
            if (currentTimeMillis - entry.getValue() > processRegisterTimeout) {
                count++;
                unregister(entry.getKey());
            }
        }
        log.info("ProcessRegister�Ƴ���ʱ�������Ϣ" + count + "��");
        return count;
    }

    public final void unregister(BytesKey key) {
        map.remove(key);
    }
}
