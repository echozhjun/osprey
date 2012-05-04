/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey.store;

import java.util.LinkedHashMap;

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
            return handler.process(eldest);
        }
        return result;
    }
}
