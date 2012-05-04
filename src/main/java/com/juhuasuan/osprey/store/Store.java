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

public interface Store {

    void add(byte[] key, byte[] data) throws IOException;

    void add(byte[] key, byte[] data, boolean force) throws IOException;

    boolean remove(byte[] key) throws IOException;

    boolean remove(byte[] key, boolean force) throws IOException;

    byte[] get(byte[] key) throws IOException;

    boolean update(byte[] key, byte[] data) throws IOException;

    int size();

    public long getMaxFileCount();

    public void setMaxFileCount(long maxFileCount);

    Iterator<byte[]> iterator() throws IOException;

    void close() throws IOException;
}
