/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey.store;

import java.io.Serializable;

public class BytesKey implements Serializable {
    private static final long serialVersionUID = -6296965387124592707L;

    private byte[] data;

    public BytesKey(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public int hashCode() {
        int h = 0;
        if (null != this.data) {
            for (int i = 0; i < this.data.length; i++) {
                h = 31 * h + data[i++];
            }
        }
        return h;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o || !(o instanceof BytesKey)) {
            return false;
        }
        BytesKey k = (BytesKey) o;
        if (null == k.getData() && null == this.getData()) {
            return true;
        }
        if (null == k.getData() || null == this.getData()) {
            return false;
        }
        if (k.getData().length != this.getData().length) {
            return false;
        }
        for (int i = 0; i < this.data.length; ++i) {
            if (this.data[i] != k.getData()[i]) {
                return false;
            }
        }
        return true;
    }
}
