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
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.juhuasuan.osprey.store.LRUHashMap.EldestEntryHandler;

public class NotifyEldestEntryHandler implements EldestEntryHandler<BytesKey, OpItem> {

    private OpItemHashMap diskMap;
    static Logger log = Logger.getLogger(NotifyEldestEntryHandler.class);

    public NotifyEldestEntryHandler(int capacity, String cacheFilePath) throws IOException {
        this.diskMap = new OpItemHashMap(2 * capacity, cacheFilePath, false);
    }

    public OpItemHashMap getDiskMap() {
        return diskMap;
    }

    public void setDiskMap(OpItemHashMap diskMap) {
        this.diskMap = diskMap;
    }

    public void close() throws IOException {
        this.diskMap.close();
    }

    public boolean process(Entry<BytesKey, OpItem> eldest) {
        try {
            return this.diskMap.put(eldest.getKey(), eldest.getValue());
        } catch (IOException e) {
            log.error("Persistent to disk error.", e);
        }
        return false;
    }

}
