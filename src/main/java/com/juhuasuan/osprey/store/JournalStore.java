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
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;


public class JournalStore implements Store {
    static Logger log = Logger.getLogger(JournalStore.class);

    public static final int FILE_SIZE = 1024 * 1024 * 64;
    public static final int HALF_DAY = 1000 * 60 * 60 * 12;
    protected static final int DEFAULT_MAX_BATCH_SIZE = 1024 * 1024 * 4;
    private final String path;
    private final String name;
    private final boolean force;

    protected IndexMap indices;
    private final Map<BytesKey, Long> lastModifiedMap = new ConcurrentHashMap<BytesKey, Long>();
    public Map<Integer, DataFile> dataFiles = new ConcurrentHashMap<Integer, DataFile>();
    protected Map<Integer, LogFile> logFiles = new ConcurrentHashMap<Integer, LogFile>();

    protected DataFile dataFile = null;
    protected LogFile logFile = null;
    private DataFileAppender dataFileAppender = null;
    private final AtomicInteger number = new AtomicInteger(0);
    private long intervalForCompact = HALF_DAY;
    private long intervalForRemove = HALF_DAY * 2 * 7;
    private volatile ScheduledExecutorService scheduledPool;

    private volatile long maxFileCount = Long.MAX_VALUE;

    protected int maxWriteBatchSize = DEFAULT_MAX_BATCH_SIZE;

    public static class InflyWriteData {
        public volatile byte[] data;
        public volatile int count;

        public InflyWriteData(byte[] data) {
            super();
            this.data = data;
            this.count = 1;
        }

    }

    public JournalStore(String path, String name, boolean force, boolean enabledIndexLRU) throws IOException {
        this(path, name, null, force, enabledIndexLRU, false);
    }

    public JournalStore(String path, String name, IndexMap indices, boolean force, boolean enabledIndexLRU) throws IOException {
        this(path, name, indices, force, enabledIndexLRU, false);
    }

    public JournalStore(String path, String name, boolean force, boolean enableIndexLRU, boolean enabledDataFileCheck) throws IOException {
        this(path, name, null, force, enableIndexLRU, false);
    }

    public JournalStore(String path, String name, IndexMap indices, boolean force, boolean enableIndexLRU, boolean enabledDataFileCheck) throws IOException {
        this.path = path;
        this.name = name;
        this.force = force;
        if (indices == null) {
            if (enableIndexLRU) {
                long maxMemory = Runtime.getRuntime().maxMemory();
                int capacity = (int) (maxMemory / 40 / 60);
                this.indices = new LRUIndexMap(capacity, this.getPath() + File.separator + name + "_indexCache", enableIndexLRU);
            } else {
                this.indices = new ConcurrentIndexMap();
            }
        } else {
            this.indices = indices;
        }
        this.dataFileAppender = new DataFileAppender(this);

        this.initLoad();

        if (null == this.dataFile || null == this.logFile) {
            this.newDataFile();
        }

        if (enabledDataFileCheck) {
            this.scheduledPool = Executors.newSingleThreadScheduledExecutor();
            this.scheduledPool.scheduleAtFixedRate(new DataFileCheckThread(), this.calcDelay(), HALF_DAY, TimeUnit.MILLISECONDS);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    JournalStore.this.close();
                } catch (IOException e) {
                    log.error("close error", e);
                }
            }
        });
    }

    public JournalStore(String path, String name) throws IOException {
        this(path, name, false, false);
    }

    @Override
    public void add(byte[] key, byte[] data) throws IOException {
        this.add(key, data, false);
    }

    @Override
    public void add(byte[] key, byte[] data, boolean force) throws IOException {
        this.checkParam(key, data);
        this.innerAdd(key, data, -1, force);

    }

    @Override
    public boolean remove(byte[] key, boolean force) throws IOException {
        return this.innerRemove(key, force);
    }

    private void reuse(byte[] key, boolean sync) throws IOException {
        byte[] value = this.get(key);
        long oldLastTime = this.lastModifiedMap.get(new BytesKey(key));
        if (value != null && this.remove(key)) {
            this.innerAdd(key, value, oldLastTime, sync);
        }
    }

    private long calcDelay() {
        Calendar date = new GregorianCalendar();
        date.setTime(new Date());
        long currentTime = date.getTime().getTime();

        date.set(Calendar.HOUR_OF_DAY, 6);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);

        long delay = date.getTime().getTime() - currentTime;
        if (delay < 0) {
            date.set(Calendar.HOUR_OF_DAY, 18);
            date.set(Calendar.MINUTE, 0);
            date.set(Calendar.SECOND, 0);
            delay = date.getTime().getTime() - currentTime;
            if (delay < 0) {
                delay += HALF_DAY;
            }
        }
        return delay;
    }

    private OpItem innerAdd(byte[] key, byte[] data, long oldLastTime, boolean sync) throws IOException {
        BytesKey k = new BytesKey(key);
        OpItem op = new OpItem();
        op.op = OpItem.OP_ADD;
        this.dataFileAppender.store(op, k, data, sync);
        this.indices.put(k, op);
        if (oldLastTime == -1) {
            this.lastModifiedMap.put(k, System.currentTimeMillis());
        } else {
            this.lastModifiedMap.put(k, oldLastTime);
        }
        return op;
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        byte[] data = null;
        BytesKey bytesKey = new BytesKey(key);
        data = this.dataFileAppender.getDataFromInFlyWrites(bytesKey);
        if (data != null) {
            return data;
        }
        OpItem op = this.indices.get(bytesKey);
        if (null != op) {

            DataFile df = this.dataFiles.get(Integer.valueOf(op.number));
            if (null != df) {
                ByteBuffer bf = ByteBuffer.wrap(new byte[op.length]);
                df.read(bf, op.offset);
                data = bf.array();
            } else {
                log.warn("����ļ���ʧ��" + op);
                this.indices.remove(bytesKey);
                this.lastModifiedMap.remove(bytesKey);
            }
        }

        return data;
    }

    @Override
    public Iterator<byte[]> iterator() throws IOException {
        final Iterator<BytesKey> it = this.indices.keyIterator();
        return new Iterator<byte[]>() {
            public boolean hasNext() {
                return it.hasNext();
            }

            public byte[] next() {
                BytesKey bk = it.next();
                if (null != bk) {
                    return bk.getData();
                }
                return null;
            }

            public void remove() {
                throw new UnsupportedOperationException("Don't support remove method.");
            }
        };
    }

    @Override
    public boolean remove(byte[] key) throws IOException {
        return this.remove(key, false);
    }

    private boolean innerRemove(byte[] key, boolean sync) throws IOException {
        boolean ret = false;
        BytesKey k = new BytesKey(key);
        OpItem op = this.indices.get(k);
        if (null != op) {
            ret = this.innerRemove(op, k, sync);
            if (ret) {
                this.indices.remove(k);
                this.lastModifiedMap.remove(k);
            }
        }
        return ret;
    }

    private boolean innerRemove(OpItem op, BytesKey bytesKey, boolean sync) throws IOException {
        DataFile df = this.dataFiles.get(Integer.valueOf(op.number));
        LogFile lf = this.logFiles.get(Integer.valueOf(op.number));
        if (null != df && null != lf) {
            OpItem o = new OpItem();
            o.key = op.key;
            o.length = op.length;
            o.number = op.number;
            o.offset = op.offset;
            o.op = OpItem.OP_DEL;
            this.dataFileAppender.remove(o, bytesKey, sync);
            return true;
        }
        return false;
    }

    private void checkParam(byte[] key, byte[] data) {
        if (null == key || null == data) {
            throw new NullPointerException("key/data can't be null");
        }
        if (key.length != 16) {
            throw new IllegalArgumentException("key.length must be 16");
        }
    }

    protected DataFile newDataFile() throws IOException {
        if (this.dataFiles.size() > this.maxFileCount) {
            throw new RuntimeException("Exceed max file count : " + this.maxFileCount);
        }
        int n = this.number.incrementAndGet();
        this.dataFile = new DataFile(new File(this.path + File.separator + this.name + "." + n), n, this.force);
        this.logFile = new LogFile(new File(this.path + File.separator + this.name + "." + n + ".log"), n, this.force);
        this.dataFiles.put(Integer.valueOf(n), this.dataFile);
        this.logFiles.put(Integer.valueOf(n), this.logFile);
        return this.dataFile;
    }

    private void checkParentDir(File parent) {
        if (!parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Can't make dir " + this.path);
        }
    }

    private void initLoad() throws IOException {
        final String nm = this.name + ".";
        File dir = new File(this.path);
        this.checkParentDir(dir);
        File[] fs = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String n) {
                return n.startsWith(nm) && !n.endsWith(".log");
            }
        });
        if (fs == null || fs.length == 0) {
            return;
        }
        List<Integer> indexList = new LinkedList<Integer>();
        for (File f : fs) {
            try {
                String fn = f.getName();
                int n = Integer.parseInt(fn.substring(nm.length()));
                indexList.add(Integer.valueOf(n));
            } catch (Exception e) {
                log.error("parse file index error" + f, e);
            }
        }

        Integer[] indices = indexList.toArray(new Integer[indexList.size()]);

        Arrays.sort(indices);

        for (Integer n : indices) {
            Map<BytesKey, OpItem> idx = new HashMap<BytesKey, OpItem>();
            File f = new File(dir, this.name + "." + n);
            DataFile df = new DataFile(f, n, this.force);
            LogFile lf = new LogFile(new File(f.getAbsolutePath() + ".log"), n, this.force);
            long size = lf.getLength() / OpItem.LENGTH;

            for (int i = 0; i < size; ++i) {
                ByteBuffer bf = ByteBuffer.wrap(new byte[OpItem.LENGTH]);
                lf.read(bf, i * OpItem.LENGTH);
                if (bf.hasRemaining()) {
                    log.warn("log file error:" + lf + ", index:" + i);
                    continue;
                }
                OpItem op = new OpItem();
                op.parse(bf.array());
                BytesKey key = new BytesKey(op.key);
                switch (op.op) {
                case OpItem.OP_ADD:
                    OpItem o = this.indices.get(key);
                    if (null != o) {
                        this.innerRemove(o, key, true);

                        this.indices.remove(key);
                        this.lastModifiedMap.remove(key);
                    }
                    boolean addRefCount = true;
                    if (idx.get(key) != null) {
                        addRefCount = false;
                    }

                    idx.put(key, op);

                    if (addRefCount) {
                        df.increment();
                    }
                    break;

                case OpItem.OP_DEL:
                    idx.remove(key);
                    df.decrement();
                    break;

                default:
                    log.warn("unknow op:" + (int) op.op);
                    break;
                }
            }
            if (df.getLength() >= FILE_SIZE && df.isUnUsed()) {
                df.delete();
                lf.delete();
            } else {
                this.dataFiles.put(n, df);
                this.logFiles.put(n, lf);
                if (!df.isUnUsed()) {
                    this.indices.putAll(idx);
                    long lastModified = lf.lastModified();
                    for (BytesKey key : idx.keySet()) {
                        this.lastModifiedMap.put(key, lastModified);
                    }
                }
            }
        }
        if (this.dataFiles.size() > 0) {
            indices = this.dataFiles.keySet().toArray(new Integer[this.dataFiles.keySet().size()]);
            Arrays.sort(indices);
            for (int i = 0; i < indices.length - 1; i++) {
                DataFile df = this.dataFiles.get(indices[i]);
                if (df.isUnUsed() || df.getLength() < FILE_SIZE) {
                    throw new IllegalStateException("IllegalStateException");
                }
            }
            Integer n = indices[indices.length - 1];
            this.number.set(n.intValue());
            this.dataFile = this.dataFiles.get(n);
            this.logFile = this.logFiles.get(n);
        }
    }

    @Override
    public int size() {
        return this.indices.size();
    }

    @Override
    public boolean update(byte[] key, byte[] data) throws IOException {
        BytesKey k = new BytesKey(key);
        OpItem op = this.indices.get(k);
        if (null != op) {
            this.indices.remove(k);
            OpItem o = this.innerAdd(key, data, -1, false);
            if (o.number != op.number) {
                this.innerRemove(op, k, false);
            } else {
                DataFile df = this.dataFiles.get(Integer.valueOf(op.number));
                df.decrement();
            }
            return true;
        }
        return false;
    }

    public String getPath() {
        return this.path;
    }

    @Override
    public void close() throws IOException {
        this.sync();
        for (DataFile df : this.dataFiles.values()) {
            try {
                df.close();
            } catch (Exception e) {
                log.warn("close error:" + df, e);
            }
        }
        this.dataFiles.clear();
        for (LogFile lf : this.logFiles.values()) {
            try {
                lf.close();
            } catch (Exception e) {
                log.warn("close error:" + lf, e);
            }
        }
        this.logFiles.clear();
        this.indices.close();
        this.lastModifiedMap.clear();
        this.dataFile = null;
        this.logFile = null;
    }

    public long getIntervalForCompact() {
        return this.intervalForCompact;
    }

    public void setIntervalForCompact(long intervalForCompact) {
        this.intervalForCompact = intervalForCompact;
    }

    public long getIntervalForRemove() {
        return this.intervalForRemove;
    }

    public void setIntervalForRemove(long intervalForRemove) {
        this.intervalForRemove = intervalForRemove;
    }

    @Override
    public long getMaxFileCount() {
        return this.maxFileCount;
    }

    @Override
    public void setMaxFileCount(long maxFileCount) {
        this.maxFileCount = maxFileCount;
    }

    public void sync() {
        this.dataFileAppender.sync();
    }

    public void check() throws IOException {
        Iterator<byte[]> keys = this.iterator();
        BytesKey key = null;
        long now = System.currentTimeMillis();
        long time;
        log.warn("Store check task start...");
        while (keys.hasNext()) {
            key = new BytesKey(keys.next());
            time = this.lastModifiedMap.get(key);
            if (this.intervalForRemove != -1 && now - time > this.intervalForRemove) {
                this.innerRemove(key.getData(), true);
            } else if (now - time > this.intervalForCompact) {
                this.reuse(key.getData(), true);
            }
        }
        log.warn("Store check task end...");
    }

    class DataFileCheckThread implements Runnable {

        public void run() {
            try {
                JournalStore.this.check();
            } catch (Exception ex) {
                JournalStore.log.warn("check error:", ex);
            }
        }
    }
}
