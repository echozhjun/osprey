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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.juhuasuan.osprey.store.JournalStore.InflyWriteData;

public class DataFileAppender {

    private volatile boolean shutdown = false;
    private boolean running = false;
    private Thread thread;
    private final Lock enqueueLock = new ReentrantLock();
    private final Condition notEmpty = this.enqueueLock.newCondition();
    private final Condition empty = this.enqueueLock.newCondition();
    protected int maxWriteBatchSize;
    protected final Map<BytesKey, InflyWriteData> inflyWrites = new ConcurrentHashMap<BytesKey, InflyWriteData>(256);
    private WriteBatch nextWriteBatch;
    private final JournalStore journal;

    public DataFileAppender(JournalStore journalStore) {
        this.maxWriteBatchSize = journalStore.maxWriteBatchSize;
        this.journal = journalStore;
    }

    public OpItem remove(OpItem opItem, BytesKey bytesKey, boolean sync) throws IOException {
        if (this.shutdown) {
            throw new RuntimeException("DataFileAppender shutdown.");
        }
        // sync = true;
        WriteCommand writeCommand = new WriteCommand(bytesKey, opItem, null, sync);
        return this.enqueueTryWait(opItem, sync, writeCommand);
    }

    public OpItem store(OpItem opItem, BytesKey bytesKey, byte[] data, boolean sync) throws IOException {
        if (this.shutdown) {
            throw new RuntimeException("DataFileAppender shutdown");
        }
        opItem.key = bytesKey.getData();
        opItem.length = data.length;
        WriteCommand writeCommand = new WriteCommand(bytesKey, opItem, data, sync);
        return this.enqueueTryWait(opItem, sync, writeCommand);
    }

    private OpItem enqueueTryWait(OpItem opItem, boolean sync, WriteCommand writeCommand) throws IOException {
        WriteBatch batch = this.enqueue(writeCommand, sync);
        if (sync) {
            try {
                batch.latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            IOException exception = batch.exception;
            if (exception != null) {
                throw exception;

            }
        }
        return opItem;
    }

    public void close() {
        this.enqueueLock.lock();
        try {
            if (!this.shutdown) {
                this.shutdown = true;
                this.running = false;
                this.empty.signalAll();
            }
        } finally {
            this.enqueueLock.unlock();
        }
        while (this.thread.isAlive()) {
            try {
                this.thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void processQueue() {
        while (true) {
            WriteBatch batch = null;
            this.enqueueLock.lock();
            try {
                while (true) {
                    if (this.nextWriteBatch != null) {
                        batch = this.nextWriteBatch;
                        this.nextWriteBatch = null;
                        break;
                    }
                    if (this.shutdown) {
                        return;
                    }
                    try {
                        this.empty.await();
                    } catch (InterruptedException e) {
                        break;
                    }

                }
                this.notEmpty.signalAll();
            } finally {
                this.enqueueLock.unlock();
            }
            if (batch != null) {
                final DataFile dataFile = batch.dataFile;
                final LogFile logFile = batch.logFile;
                final List<WriteCommand> cmdList = batch.cmdList;
                try {
                    this.writeDataAndLog(batch, dataFile, logFile, cmdList);
                    this.processRemove(batch, dataFile, logFile);
                } finally {
                    batch.latch.countDown();
                }
            }

        }
    }

    private void processRemove(WriteBatch batch, DataFile df, LogFile lf) {

        if (df != null && lf != null) {
            df.decrement(batch.removeOPCount);
            this.enqueueLock.lock();
            try {
                if (df.getLength() >= JournalStore.FILE_SIZE && df.isUnUsed()) {
                    if (this.journal.dataFile == df) { // �ж�����ǵ�ǰ�ļ�������µ�
                        this.journal.newDataFile();
                    }
                    this.journal.dataFiles.remove(Integer.valueOf(df.getNumber()));
                    this.journal.logFiles.remove(Integer.valueOf(df.getNumber()));
                    // System.out.println("delete " + df.getNumber());
                    // System.out.println(batch.cmdList.get(0).opItem);
                    df.delete();
                    lf.delete();
                }
            } catch (Exception e) {
                if (e instanceof IOException) {
                    batch.exception = (IOException) e;
                } else {
                    batch.exception = new IOException(e);
                }
            } finally {
                this.enqueueLock.unlock();
            }
        }
    }

    public byte[] getDataFromInFlyWrites(BytesKey key) {
        InflyWriteData inflyWriteData = this.inflyWrites.get(key);
        if (inflyWriteData != null && inflyWriteData.count > 0) {
            return inflyWriteData.data;
        } else {
            return null;
        }

    }

    private void writeDataAndLog(WriteBatch batch, final DataFile dataFile, final LogFile logFile, final List<WriteCommand> dataList) {
        ByteBuffer dataBuf = null;
        // Contains op add
        if (batch.dataSize > 0) {
            dataBuf = ByteBuffer.allocate(batch.dataSize);
        }
        ByteBuffer opBuf = ByteBuffer.allocate(dataList.size() * OpItem.LENGTH);
        for (WriteCommand cmd : dataList) {
            opBuf.put(cmd.opItem.toByte());
            if (cmd.opItem.op == OpItem.OP_ADD) {
                dataBuf.put(cmd.data);
            }
        }
        if (dataBuf != null) {
            dataBuf.flip();
        }
        opBuf.flip();
        try {
            if (dataBuf != null) {
                dataFile.write(dataBuf);
            }
            logFile.write(opBuf);
        } catch (IOException e) {
            batch.exception = e;
        }
        this.enqueueLock.lock();
        try {
            for (WriteCommand cmd : dataList) {
                if (!cmd.sync && cmd.opItem.op == OpItem.OP_ADD) {
                    InflyWriteData inflyWriteData = this.inflyWrites.get(cmd.bytesKey);
                    if (inflyWriteData != null) {
                        // decrease reference count
                        inflyWriteData.count--;
                        // remove it if there is no reference
                        if (inflyWriteData.count <= 0) {
                            this.inflyWrites.remove(cmd.bytesKey);
                        }
                    }
                }
            }
        } finally {
            this.enqueueLock.unlock();
        }

    }

    final Condition notSync = this.enqueueLock.newCondition();

    public void sync() {
        this.enqueueLock.lock();
        try {
            while (this.nextWriteBatch != null) {
                try {
                    this.notEmpty.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            for (DataFile df : this.journal.dataFiles.values()) {
                try {
                    df.sync(this.notSync);
                } catch (Exception e) {

                }
            }
        } finally {
            this.enqueueLock.unlock();
        }
    }

    private WriteBatch enqueue(WriteCommand writeCommand, boolean sync) throws IOException {
        WriteBatch result = null;
        this.enqueueLock.lock();
        try {
            this.startAppendThreadIfNessary();
            if (this.nextWriteBatch == null) {
                result = this.newWriteBatch(writeCommand);
                this.empty.signalAll();
            } else {
                if (this.nextWriteBatch.canAppend(writeCommand)) {
                    this.nextWriteBatch.append(writeCommand);
                    result = this.nextWriteBatch;
                } else {
                    while (this.nextWriteBatch != null) {
                        try {
                            this.notEmpty.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    result = this.newWriteBatch(writeCommand);
                    this.empty.signalAll();
                }
            }
            if (!sync) {
                InflyWriteData inflyWriteData = this.inflyWrites.get(writeCommand.bytesKey);
                switch (writeCommand.opItem.op) {
                case OpItem.OP_ADD:
                    if (inflyWriteData == null) {
                        this.inflyWrites.put(writeCommand.bytesKey, new InflyWriteData(writeCommand.data));
                    } else {
                        // update and increase reference count;
                        inflyWriteData.data = writeCommand.data;
                        inflyWriteData.count++;
                    }
                    break;
                case OpItem.OP_DEL:
                    if (inflyWriteData != null) {
                        this.inflyWrites.remove(writeCommand.bytesKey);
                    }
                }

            }
            return result;
        } finally {
            this.enqueueLock.unlock();
        }

    }

    private WriteBatch newWriteBatch(WriteCommand writeCommand) throws IOException {
        WriteBatch result = null;
        if (writeCommand.opItem.op == OpItem.OP_ADD) {
            if (this.journal.indices.containsKey(writeCommand.bytesKey)) {
                throw new IOException("Duplicated key");
            }
            DataFile dataFile = this.getDataFile();
            writeCommand.opItem.offset = dataFile.position();
            writeCommand.opItem.number = dataFile.getNumber();
            dataFile.forward(writeCommand.data.length);
            this.nextWriteBatch = new WriteBatch(writeCommand, dataFile, this.journal.logFile);
            result = this.nextWriteBatch;
        } else {
            DataFile dataFile = this.journal.dataFiles.get(writeCommand.opItem.number);
            LogFile logFile = this.journal.logFiles.get(writeCommand.opItem.number);
            if (dataFile != null && logFile != null) {
                this.nextWriteBatch = new WriteBatch(writeCommand, dataFile, logFile);
                result = this.nextWriteBatch;
            } else {
                throw new IOException("Log file or Data file does not exist." + writeCommand.opItem.number);
            }
        }
        return result;
    }

    private void startAppendThreadIfNessary() {
        if (!this.running) {
            this.running = true;
            this.thread = new Thread() {
                @Override
                public void run() {
                    DataFileAppender.this.processQueue();
                }
            };
            this.thread.setPriority(Thread.MAX_PRIORITY);
            this.thread.setDaemon(true);
            this.thread.setName("Store4j file appender");
            this.thread.start();
        }
    }

    private DataFile getDataFile() throws IOException {
        DataFile dataFile = this.journal.dataFile;
        if (dataFile.getLength() >= JournalStore.FILE_SIZE) {
            dataFile = this.journal.newDataFile();
        }
        return dataFile;
    }

    private class WriteCommand {
        final BytesKey bytesKey;
        final OpItem opItem;
        final byte[] data;
        final boolean sync;

        public WriteCommand(BytesKey bytesKey, OpItem opItem, byte[] data, boolean sync) {
            super();
            this.bytesKey = bytesKey;
            this.opItem = opItem;
            this.data = data;
            this.sync = sync;
        }

        @Override
        public String toString() {
            return this.opItem.toString();
        }
    }

    private class WriteBatch {
        final CountDownLatch latch = new CountDownLatch(1);
        final List<WriteCommand> cmdList = new ArrayList<WriteCommand>();
        int removeOPCount;
        final DataFile dataFile;
        final LogFile logFile;
        int dataSize;
        long offset = -1;
        volatile IOException exception;
        final int number;

        public WriteBatch(WriteCommand writeCommand, DataFile dataFile, LogFile logFile) {
            super();
            this.dataFile = dataFile;
            this.number = writeCommand.opItem.number;
            this.logFile = logFile;
            switch (writeCommand.opItem.op) {
            case OpItem.OP_DEL:
                this.removeOPCount++;
                break;
            case OpItem.OP_ADD:
                this.offset = writeCommand.opItem.offset;
                this.dataSize += writeCommand.data.length;
                this.dataFile.increment();
                break;
            default:
                throw new RuntimeException("Unknow op type " + writeCommand.opItem);
            }
            this.cmdList.add(writeCommand);

        }

        public boolean canAppend(WriteCommand command) throws IOException {
            switch (command.opItem.op) {
            case OpItem.OP_DEL:
                if (command.opItem.number != this.number) {
                    return false;
                }
                break;
            case OpItem.OP_ADD:
                if (this.dataFile.getLength() + command.data.length >= JournalStore.FILE_SIZE) {
                    return false;
                }
                if (this.dataSize + command.data.length >= DataFileAppender.this.maxWriteBatchSize) {
                    return false;
                }
                break;
            default:
                throw new RuntimeException("Unknow op type " + command.opItem);
            }

            return true;
        }

        public void append(WriteCommand writeCommand) throws IOException {
            switch (writeCommand.opItem.op) {
            case OpItem.OP_ADD:
                if (this.offset == -1) {
                    this.offset = this.dataFile.position();
                    writeCommand.opItem.offset = this.dataFile.position();
                    writeCommand.opItem.number = this.dataFile.getNumber();
                    this.dataFile.forward(writeCommand.data.length);
                    this.dataSize += writeCommand.data.length;
                } else {
                    writeCommand.opItem.offset = this.offset + this.dataSize;
                    writeCommand.opItem.number = this.dataFile.getNumber();
                    this.dataFile.forward(writeCommand.data.length);
                    this.dataSize += writeCommand.data.length;
                }
                this.dataFile.increment();
                break;
            case OpItem.OP_DEL:
                this.removeOPCount++;
                break;
            default:
                throw new RuntimeException("Unknow op type " + writeCommand.opItem);
            }
            this.cmdList.add(writeCommand);
        }

    }
}
