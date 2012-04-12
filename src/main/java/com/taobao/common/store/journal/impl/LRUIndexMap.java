package com.taobao.common.store.journal.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.common.store.journal.IndexMap;
import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.LRUHashMap;

/**
 * 
 * ª˘”⁄LRUµƒIndexMap£¨ø…Ω´LRUÃÊªª≥ˆ¿¥µƒOpItem¥Ê¥¢”⁄¥≈≈Ãª∫¥Ê
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-10-20 …œŒÁ11:04:37
 */

public class LRUIndexMap implements IndexMap {
    private final Lock lock = new ReentrantLock();
    private LRUHashMap<BytesKey, OpItem> map;
    private NotifyEldestEntryHandler handler;
    private boolean enableLRU;

    public LRUIndexMap(int capacity, String cacheFilePath, boolean enableLRU) throws IOException {
        this.enableLRU = enableLRU;
        map = new LRUHashMap<BytesKey, OpItem>(capacity, enableLRU);
        handler = new NotifyEldestEntryHandler(capacity, cacheFilePath);
        map.setHandler(handler);
    }

    public void close() throws IOException {
        this.lock.lock();
        try {
            this.handler.close();
        } finally {
            this.lock.unlock();
        }
    }

    public LRUHashMap<BytesKey, OpItem> getMap() {
        return map;
    }

    public NotifyEldestEntryHandler getHandler() {
        return handler;
    }

    public boolean containsKey(BytesKey key) {
        this.lock.lock();
        try {

            return map.containsKey(key) || (enableLRU && this.handler.getDiskMap().get(key) != null);
        } catch (IOException e) {
            throw new IllegalStateException("≤È—ØKey ß∞‹", e);
        } finally {
            this.lock.unlock();
        }
    }

    public OpItem get(BytesKey key) {
        this.lock.lock();
        try {
            OpItem result = map.get(key);
            if (result == null && enableLRU) {
                result = handler.getDiskMap().get(key);
            }
            return result;
        } catch (IOException e) {
            throw new IllegalStateException("∑√Œ ¥≈≈Ãª∫¥Ê ß∞‹", e);
        } finally {
            this.lock.unlock();
        }

    }

    class LRUIndexMapItreator implements Iterator<BytesKey> {

        private Iterator<BytesKey> mapIt;
        private Iterator<BytesKey> diskMapIt;
        private volatile boolean enterDisk;
        private BytesKey currentKey;

        public LRUIndexMapItreator(Iterator<BytesKey> mapIt, Iterator<BytesKey> diskMapIt) {
            super();
            this.mapIt = mapIt;
            this.diskMapIt = diskMapIt;
        }

        public boolean hasNext() {
            lock.lock();
            try {
                if (mapIt.hasNext()) {
                    return true;
                }
                if (enableLRU) {
                    if (!enterDisk) {
                        enterDisk = true;
                    }
                    return diskMapIt.hasNext();
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        public BytesKey next() {
            lock.lock();
            try {
                BytesKey result = null;
                if (!enterDisk) {
                    result = mapIt.next();
                } else {
                    result = diskMapIt.next();
                }
                this.currentKey = result;
                return result;
            } finally {
                lock.unlock();
            }
        }

        public void remove() {
            lock.lock();
            try {
                if (currentKey == null) {
                    throw new IllegalStateException("The next method is not been called");
                }
                LRUIndexMap.this.remove(this.currentKey);
            } finally {
                lock.unlock();
            }
        }

    }

    public Iterator<BytesKey> keyIterator() {
        lock.lock();
        try {
            return new LRUIndexMapItreator(new HashSet<BytesKey>(map.keySet()).iterator(), handler.getDiskMap().iterator());
        } finally {
            lock.unlock();
        }
    }

    public void put(BytesKey key, OpItem opItem) {
        lock.lock();
        try {
            this.map.put(key, opItem);
        } finally {
            lock.unlock();
        }
    }

    public void putAll(Map<BytesKey, OpItem> map) {
        lock.lock();
        try {
            this.map.putAll(map);
        } finally {
            lock.unlock();
        }
    }

    public void remove(BytesKey key) {
        lock.lock();
        try {
            OpItem result = map.remove(key);
            if (result == null && enableLRU) {
                try {
                    handler.getDiskMap().remove(key);
                } catch (IOException e) {
                    throw new IllegalStateException("∑√Œ ¥≈≈Ãª∫¥Ê ß∞‹", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return map.size() + handler.getDiskMap().size();
        } finally {
            lock.unlock();
        }
    }

}
