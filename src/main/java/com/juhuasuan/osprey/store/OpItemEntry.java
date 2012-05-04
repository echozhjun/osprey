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
import java.nio.MappedByteBuffer;


public class OpItemEntry {
    public static final int SIZE = 33 + 1;
    private OpItem opItem;
    private boolean deleted;
    private byte channelIndex;
    static final byte[] deltedBuffer = new byte[1];

    public boolean isLoaded() {
        return this.opItem != null;
    }

    public void unload() {
        this.opItem = null;
    }

    public byte getChannelIndex() {
        return channelIndex;
    }

    public void setChannelIndex(byte channelIndex) {
        this.channelIndex = channelIndex;
    }

    public void load(MappedByteBuffer mappedByteBuffer, int offset, boolean loadItem) throws IOException {
        if (this.deleted) {
            return;
        }
        mappedByteBuffer.position(offset);
        if (!loadItem) {
            byte data = mappedByteBuffer.get();
            this.deleted = (data == (byte) 1 ? true : false);
        } else {
            byte[] bytes = new byte[SIZE];
            mappedByteBuffer.get(bytes, 0, SIZE);
            this.deleted = (bytes[0] == (byte) 1 ? true : false);
            this.opItem = new OpItem();
            this.opItem.parse(bytes, 1, bytes.length - 1);
        }
    }

    public byte[] encode() {
        if (this.opItem != null) {
            byte[] buffer = new byte[OpItemEntry.SIZE];
            if (this.deleted) {
                buffer[0] = 1;
            } else {
                buffer[0] = 0;
            }
            byte[] data = this.opItem.toByte();
            System.arraycopy(data, 0, buffer, 1, data.length);
            return buffer;
        } else {
            return null;
        }
    }

    public OpItemEntry(OpItem opItem, boolean deleted) {
        super();
        this.opItem = opItem;
        this.deleted = deleted;
    }

    public OpItem getOpItem() {
        return opItem;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
}
