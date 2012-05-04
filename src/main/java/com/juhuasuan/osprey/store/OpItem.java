/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey.store;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class OpItem {
    public static final byte OP_ADD = 1;
    public static final byte OP_DEL = 2;

    public static final int KEY_LENGTH = 16;
    public static final int LENGTH = KEY_LENGTH + 1 + 4 + 8 + 4;

    byte op;
    byte[] key;
    int number;
    volatile long offset;
    int length;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.key);
        result = prime * result + this.length;
        result = prime * result + this.number;
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        result = prime * result + this.op;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        OpItem other = (OpItem) obj;
        if (!Arrays.equals(this.key, other.key)) {
            return false;
        }
        if (this.length != other.length) {
            return false;
        }
        if (this.number != other.number) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        if (this.op != other.op) {
            return false;
        }
        return true;
    }

    public byte[] toByte() {
        byte[] data = new byte[LENGTH];
        ByteBuffer bf = ByteBuffer.wrap(data);
        bf.put(this.key);
        bf.put(this.op);
        bf.putInt(this.number);
        bf.putLong(this.offset);
        bf.putInt(this.length);
        bf.flip();
        return bf.array();
    }

    public byte getOp() {
        return this.op;
    }

    public void setOp(byte op) {
        this.op = op;
    }

    public byte[] getKey() {
        return this.key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public int getNumber() {
        return this.number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public long getOffset() {
        return this.offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getLength() {
        return this.length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void parse(byte[] data) {
        this.parse(data, 0, data.length);
    }

    public void parse(byte[] data, int offset, int length) {
        ByteBuffer bf = ByteBuffer.wrap(data, offset, length);
        this.key = new byte[16];
        bf.get(this.key);
        this.op = bf.get();
        this.number = bf.getInt();
        this.offset = bf.getLong();
        this.length = bf.getInt();
    }

    public void parse(ByteBuffer bf) {
        this.key = new byte[16];
        bf.get(this.key);
        this.op = bf.get();
        this.number = bf.getInt();
        this.offset = bf.getLong();
        this.length = bf.getInt();
    }

    @Override
    public String toString() {
        return "OpItem number:" + this.number + ", op:" + (int) this.op + ", offset:" + this.offset + ", length:" + this.length;
    }
}
