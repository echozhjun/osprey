package com.taobao.common.store.journal;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.taobao.common.store.util.BytesKey;

/**
 * 
 * 
 * Ë÷ÒýMap
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-10-20 ÉÏÎç10:49:42
 */

public interface IndexMap {
    public void put(BytesKey key, OpItem opItem);

    public void remove(BytesKey key);

    public OpItem get(BytesKey key);

    public int size();

    public boolean containsKey(BytesKey key);

    public Iterator<BytesKey> keyIterator();

    public void putAll(Map<BytesKey, OpItem> map);

    public void close() throws IOException;
}
