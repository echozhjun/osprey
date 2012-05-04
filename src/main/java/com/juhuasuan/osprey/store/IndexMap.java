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
