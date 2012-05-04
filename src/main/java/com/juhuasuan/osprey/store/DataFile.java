/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

class DataFile {
    private final File file;
    private final AtomicInteger referenceCount = new AtomicInteger(0);
    protected FileChannel fc;
    private final int number;
    private volatile long currentPos;

    DataFile(File file, int number) throws IOException {
        this(file, number, false);
    }

    DataFile(File file, int number, boolean force) throws IOException {
        this.file = file;
        this.fc = new RandomAccessFile(file, force ? "rws" : "rw").getChannel();
        this.fc.position(this.fc.size());
        this.currentPos = this.fc.position();
        this.number = number;
    }

    int getNumber() {
        return this.number;
    }

    long getLength() throws IOException {
        return this.currentPos;
    }

    long position() throws IOException {
        return this.currentPos;
    }

    void forward(long offset) {
        this.currentPos += offset;
    }

    void sync(Condition condition) throws Exception {
        while (this.fc.position() < this.currentPos) {
            condition.await(1, TimeUnit.SECONDS);
        }
        this.fc.force(true);
    }

    long lastModified() throws IOException {
        return this.file.lastModified();
    }

    boolean delete() throws IOException {
        this.close();
        return this.file.delete();
    }

    void force() throws IOException {
        this.fc.force(true);
    }

    void close() throws IOException {
        this.fc.close();
    }

    void read(ByteBuffer bf) throws IOException {
        while (bf.hasRemaining()) {
            int l = this.fc.read(bf);
            if (l < 0) {
                break;
            }
        }
    }

    void read(ByteBuffer bf, long offset) throws IOException {
        int size = 0;
        int l = 0;
        while (bf.hasRemaining()) {
            l = this.fc.read(bf, offset + size);
            if (l < 0) {
                // ��ݻ�δд�룬æ�ȴ�
                if (offset < this.currentPos) {
                    continue;
                } else {
                    break;
                }
            }
            size += l;
        }
    }

    long write(ByteBuffer bf) throws IOException {
        while (bf.hasRemaining()) {
            int l = this.fc.write(bf);
            if (l < 0) {
                break;
            }
        }
        return this.fc.position();
    }

    void write(long offset, ByteBuffer bf) throws IOException {
        int size = 0;
        while (bf.hasRemaining()) {
            int l = this.fc.write(bf, offset + size);
            size += l;
            if (l < 0) {
                break;
            }
        }
    }

    int increment() {
        return this.referenceCount.incrementAndGet();
    }

    int increment(int n) {
        return this.referenceCount.addAndGet(n);
    }

    int decrement() {
        return this.referenceCount.decrementAndGet();
    }

    int decrement(int n) {
        return this.referenceCount.addAndGet(-n);
    }

    boolean isUnUsed() {
        return this.getReferenceCount() <= 0;
    }

    int getReferenceCount() {
        return this.referenceCount.get();
    }

    @Override
    public String toString() {
        String result = null;
        try {
            result = this.file.getName() + " , length = " + this.getLength() + " refCount = " + this.referenceCount + " position:" + this.fc.position();
        } catch (IOException e) {
            result = e.getMessage();
        }
        return result;
    }
}
