/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey.store;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


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
