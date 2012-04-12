/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package com.taobao.common.store.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

/**
 * ������һ�������ļ�
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
class DataFile {
    private final File file;
    private final AtomicInteger referenceCount = new AtomicInteger(0);
    protected FileChannel fc;
    private final int number;
    private volatile long currentPos;

    /**
     * ���캯�������ָ�����ļ������ҽ�ָ��ָ���ļ���β
     * 
     * @param file
     * @throws IOException
     */
    DataFile(File file, int number) throws IOException {
        this(file, number, false);
    }

    /**
     * ���캯�������ָ�����ļ������ҽ�ָ��ָ���ļ���β
     * 
     * @param file
     * @throws IOException
     */
    DataFile(File file, int number, boolean force) throws IOException {
        this.file = file;
        this.fc = new RandomAccessFile(file, force ? "rws" : "rw").getChannel();
        // ָ���Ƶ����
        this.fc.position(this.fc.size());
        this.currentPos = this.fc.position();
        this.number = number;
    }

    int getNumber() {
        return this.number;
    }

    /**
     * ����ļ��Ĵ�С
     * 
     * @return �ļ��Ĵ�С
     * @throws IOException
     */
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

    /**
     * ��ȡ�ļ�����޸�ʱ��
     * 
     * @return
     * @throws IOException
     */
    long lastModified() throws IOException {
        return this.file.lastModified();
    }

    /**
     * ɾ���ļ�
     * 
     * @return �Ƿ�ɾ���ɹ�
     * @throws IOException
     */
    boolean delete() throws IOException {
        this.close();
        return this.file.delete();
    }

    /**
     * ǿ�ƽ�����д��Ӳ��
     * 
     * @throws IOException
     */
    void force() throws IOException {
        this.fc.force(true);
    }

    /**
     * �ر��ļ�
     * 
     * @throws IOException
     */
    void close() throws IOException {
        this.fc.close();
    }

    /**
     * ���ļ���ȡ���ݵ�bf��ֱ���������߶����ļ���β�� <br />
     * �ļ���ָ�������ƶ�bf�Ĵ�С
     * 
     * @param bf
     * @throws IOException
     */
    void read(ByteBuffer bf) throws IOException {
        while (bf.hasRemaining()) {
            int l = this.fc.read(bf);
            if (l < 0) {
                break;
            }
        }
    }

    /**
     * ���ļ����ƶ�λ�ö�ȡ���ݵ�bf��ֱ���������߶����ļ���β�� <br />
     * �ļ�ָ�벻���ƶ�
     * 
     * @param bf
     * @param offset
     * @throws IOException
     */
    void read(ByteBuffer bf, long offset) throws IOException {
        int size = 0;
        int l = 0;
        while (bf.hasRemaining()) {
            l = this.fc.read(bf, offset + size);
            if (l < 0) {
                // ���ݻ�δд�룬æ�ȴ�
                if (offset < this.currentPos) {
                    continue;
                } else {
                    break;
                }
            }
            size += l;
        }
    }

    /**
     * д��bf���ȵ����ݵ��ļ����ļ�ָ�������ƶ�
     * 
     * @param bf
     * @return д�����ļ�position
     * @throws IOException
     */
    long write(ByteBuffer bf) throws IOException {
        while (bf.hasRemaining()) {
            int l = this.fc.write(bf);
            if (l < 0) {
                break;
            }
        }
        return this.fc.position();
    }

    /**
     * ��ָ��λ��д��bf���ȵ����ݵ��ļ����ļ�ָ��<b>����</b>����ƶ�
     * 
     * @param offset
     * @param bf
     * @throws IOException
     */
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

    /**
     * ���ļ�����һ�����ü���
     * 
     * @return ���Ӻ�����ü���
     */
    int increment() {
        return this.referenceCount.incrementAndGet();
    }

    int increment(int n) {
        return this.referenceCount.addAndGet(n);
    }

    /**
     * ���ļ�����һ�����ü���
     * 
     * @return ���ٺ�����ü���
     */
    int decrement() {
        return this.referenceCount.decrementAndGet();
    }

    int decrement(int n) {
        return this.referenceCount.addAndGet(-n);
    }

    /**
     * �ļ��Ƿ���ʹ�ã����ü����Ƿ���0�ˣ�
     * 
     * @return �ļ��Ƿ���ʹ��
     */
    boolean isUnUsed() {
        return this.getReferenceCount() <= 0;
    }

    /**
     * ������ü�����ֵ
     * 
     * @return ���ü�����ֵ
     */
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
