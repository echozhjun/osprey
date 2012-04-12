package com.taobao.common.store.util;

import java.util.LinkedHashMap;

/**
 * *
 * 
 * @author dennis
 * 
 * @param <K>
 * @param <V>
 */
public class LRUHashMap<K, V> extends LinkedHashMap<K, V> {
    private final int maxCapacity;

    static final long serialVersionUID = 438971390573954L;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private transient EldestEntryHandler<K, V> handler;

    private boolean enableLRU;

    public void setHandler(EldestEntryHandler<K, V> handler) {
        this.handler = handler;
    }

    public interface EldestEntryHandler<K, V> {
        public boolean process(java.util.Map.Entry<K, V> eldest);
    }

    public LRUHashMap() {
        this(1000, true);
    }

    public LRUHashMap(int maxCapacity, boolean enableLRU) {
        super(maxCapacity, DEFAULT_LOAD_FACTOR, true);
        this.maxCapacity = maxCapacity;
        this.enableLRU = enableLRU;
    }

    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
        if (!this.enableLRU) {
            return false;
        }
        boolean result = size() > maxCapacity;
        if (result && handler != null) {
            // 成功存入磁盘，即从内存移除，否则继续保留在保存
            return handler.process(eldest);
        }
        return result;
    }
}
