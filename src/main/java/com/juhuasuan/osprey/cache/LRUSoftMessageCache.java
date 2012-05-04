/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey.cache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.juhuasuan.osprey.MessageInStore;
import com.juhuasuan.osprey.store.BytesKey;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-3-15
 * @version 1.0
 */
public class LRUSoftMessageCache {
    private final LRUSoftHashMap<BytesKey, MessageInStore> map;

    private final Lock lock = new ReentrantLock();

    public LRUSoftMessageCache(int lowWaterMark, int highWaterMark) {
        if (lowWaterMark <= 0) {
            throw new IllegalArgumentException("lowWaterMark<=0");
        }
        if (highWaterMark <= 0) {
            throw new IllegalArgumentException("highWaterMark<=0");
        }
        if (highWaterMark < lowWaterMark) {
            throw new IllegalArgumentException("highWaterMark<lowWaterMark");
        }

        this.map = new LRUSoftHashMap<BytesKey, MessageInStore>(lowWaterMark, highWaterMark);

    }

    public int getLowWaterMark() {
        lock.lock();
        try {
            return map.getLowWaterMark();
        } finally {
            lock.unlock();
        }
    }

    public void setLowWaterMark(int lowWaterMark) {
        lock.lock();
        try {
            map.setLowWaterMark(lowWaterMark);
        } finally {
            lock.unlock();
        }
    }

    public int getHighWaterMark() {
        lock.lock();
        try {
            return map.getHighWaterMark();
        } finally {
            lock.unlock();
        }
    }

    public void setHighWaterMark(int highWaterMark) {
        lock.lock();
        try {
            map.setHighWaterMark(highWaterMark);
        } finally {
            lock.unlock();
        }
    }

    public MessageInStore put(byte[] msgId, MessageInStore messageInStore4j) {
        lock.lock();
        try {
            return map.put(new BytesKey(msgId), messageInStore4j);
        } finally {
            lock.unlock();
        }
    }

    public MessageInStore remove(byte[] msgId) {
        lock.lock();
        try {
            return map.remove(new BytesKey(msgId));
        } finally {
            lock.unlock();
        }
    }

    public MessageInStore get(byte[] msgId) {
        lock.lock();
        try {
            return map.get(new BytesKey(msgId));
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        lock.lock();
        try {
            map.clear();
        } finally {
            lock.unlock();
        }
    }

    public long getCurrentCacheSize() {
        lock.lock();
        try {
            return map.size();
        } finally {
            lock.unlock();
        }
    }
}
