/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey.cache;

import java.io.Serializable;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-15
 * @version 1.0
 */
public class LRUSoftHashMap<K, V> extends LinkedHashMap<K, V> implements LocalCache<K, V> {

    public static final int DEFAULT_INITIAL_CAPACITY = 256;

    public static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private static final long serialVersionUID = 1846465456485877878L;

    private SoftReferenceHashMap softMap = new SoftReferenceHashMap();

    private int lowWaterMark, highWaterMark;

    public LRUSoftHashMap(int lowWaterMark, int highWaterMark) {
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, true);
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean result = this.hardSize() > getLowWaterMark();
        if (result) {
            if (this.hardSize() + softMap.size() <= this.highWaterMark) {
                softMap.put(eldest.getKey(), eldest.getValue());
            }
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        V result = super.get(key);
        if (result == null) {
            result = softMap.get((K) key);
        }
        return result;
    }

    @Override
    public void clear() {
        softMap.clear();
        super.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        return super.containsKey(key) || this.softMap.containsKey((K) key);
    }

    @Override
    public boolean isEmpty() {
        return super.isEmpty() && this.softMap.isEmpty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        softMap.remove((K) key);
        return super.remove(key);
    }

    @Override
    public final int size() {
        return softSize() + this.hardSize();
    }

    public final int hardSize() {
        return super.size();
    }

    public final int softSize() {
        return this.softMap.size();
    }

    public final int getLowWaterMark() {
        return lowWaterMark;
    }

    public final void setLowWaterMark(int lowWaterMark) {
        this.lowWaterMark = lowWaterMark;
    }

    public final int getHighWaterMark() {
        return highWaterMark;
    }

    public final void setHighWaterMark(int highWaterMark) {
        this.highWaterMark = highWaterMark;
    }

    public long getCurrentCacheSize() {
        return size();
    }

    public class SoftReferenceHashMap implements Serializable {
        static final long serialVersionUID = 3578712168613500464L;
        private Map<K, SoftValue> map;
        private transient ReferenceQueue<V> queue = new ReferenceQueue<V>();

        public SoftReferenceHashMap() {
            this.map = new HashMap<K, SoftValue>();
        }

        public SoftReferenceHashMap(int initialCapacity) {
            this.map = new HashMap<K, SoftValue>(initialCapacity);
        }

        public boolean containsKey(K key) {
            expungeStaleValues();
            return this.map.containsKey(key);
        }

        public V put(K key, V value) {
            expungeStaleValues();
            SoftValue softValue = new SoftValue(value, queue, key);
            SoftValue old = map.put(key, softValue);
            if (old != null) {
                return old.get();
            } else {
                return null;
            }
        }

        public V get(K key) {
            expungeStaleValues();
            SoftValue softValue = map.get(key);
            if (softValue != null) {
                return softValue.get();
            } else {
                return null;
            }
        }

        public boolean isEmpty() {
            expungeStaleValues();
            return map.isEmpty();
        }

        public int size() {
            expungeStaleValues();
            return map.size();
        }

        public void clear() {
            expungeStaleValues();
            map.clear();
        }

        public void remove(K key) {
            expungeStaleValues();
            map.remove(key);
        }

        @SuppressWarnings("unchecked")
        private void expungeStaleValues() {
            SoftValue softValue;
            while ((softValue = (SoftValue) queue.poll()) != null) {
                K staleKey = softValue.key;
                map.remove(staleKey);
                softValue.clear();
                softValue = null;
            }
        }

        class SoftValue extends SoftReference<V> {
            K key;

            public SoftValue(V referent, ReferenceQueue<V> q, K key) {
                super(referent, q);
                this.key = key;
            }

        }
    }

}
