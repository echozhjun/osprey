package com.taobao.common.store.journal.impl;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.LRUHashMap.EldestEntryHandler;

/**
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-10-20 …œŒÁ11:17:23
 */

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
            // ≥¢ ‘¥Ê»Î¥≈≈Ã
            return this.diskMap.put(eldest.getKey(), eldest.getValue());
        } catch (IOException e) {
            e.printStackTrace();
            log.error("–¥»Î¥≈≈Ãª∫¥Ê ß∞‹", e);
        }
        return false;
    }

}
