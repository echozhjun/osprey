package com.taobao.common.store.journal.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.common.store.journal.IndexMap;
import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.util.BytesKey;

/**
 * 
 * @author boyan *
 */

public class ConcurrentIndexMap implements IndexMap {
    private ConcurrentHashMap<BytesKey, OpItem> map;

    public ConcurrentIndexMap() {
        this.map = new ConcurrentHashMap<BytesKey, OpItem>();
    }

    public boolean containsKey(BytesKey key) {
        return this.map.containsKey(key);
    }

    public OpItem get(BytesKey key) {
        return this.map.get(key);
    }

    public Iterator<BytesKey> keyIterator() {
        return this.map.keySet().iterator();
    }

    public void put(BytesKey key, OpItem opItem) {
        this.map.put(key, opItem);
    }

    public void putAll(Map<BytesKey, OpItem> map) {
        this.map.putAll(map);
    }

    public void remove(BytesKey key) {
        this.map.remove(key);
    }

    public int size() {
        return this.map.size();
    }

    public void close() throws IOException {
        this.map.clear();
    }

}
