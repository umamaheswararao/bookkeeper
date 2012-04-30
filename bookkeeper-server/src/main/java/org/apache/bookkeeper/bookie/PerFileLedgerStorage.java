/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutionException;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.CacheLoader;

import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.meta.LedgerManager;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PerFileLedgerStorage implements LedgerStorage {
    final static Logger LOG = LoggerFactory.getLogger(PerFileLedgerStorage.class);

    static class LedgerFile {
        AtomicBoolean dirty = new AtomicBoolean(false);
        final FileChannel ledger; 

        LedgerFile(FileChannel ledger) {
            this.ledger = ledger;
        }

        FileChannel getLedgerFile() {
            return ledger;
        }

        void markDirty() {
            dirty.set(true);
        }
        void sync() throws IOException {
            if (dirty.get()) {
                ledger.force(false);
                dirty.set(false);
            }
        }

        void close() {
            try {
                sync();
                ledger.close();
            } catch (IOException ioe) {
                LOG.error("Error closing ledger file " + ledger, ioe);
            }
        }
    }

    final LoadingCache<Long, LedgerFile> fileCache;
    final LedgerCache ledgerIndex;
    boolean somethingWritten = false;

    final File ledgerDirectories[];

    PerFileLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager)
            throws IOException {
        this.ledgerDirectories = Bookie.getCurrentDirectories(conf.getLedgerDirs());

        fileCache = CacheBuilder.newBuilder()
            .maximumSize(conf.getOpenFileLimit()/2)
            .removalListener(new RemovalListener<Long, LedgerFile> () {
                    public void onRemoval(RemovalNotification<Long,LedgerFile> notification) {
                        LOG.debug("Evicting ledger {}", notification.getKey());
                        notification.getValue().close();
                    }
                })
            .build(
                   new CacheLoader<Long, LedgerFile>() {
                       public LedgerFile load(Long ledgerId) throws Exception {
                           LOG.debug("Creating ledger {}", ledgerId);
                           return createLedgerFile(ledgerId);
                       }
                   });

        ledgerIndex = new LedgerCacheImpl(conf, ledgerManager);
    }

    public void start() {
    }

    public void shutdown() throws InterruptedException {
    }

    public boolean ledgerExists(long ledgerId) throws IOException {
        return ledgerIndex.ledgerExists(ledgerId);
    }

    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        ledgerIndex.setMasterKey(ledgerId, masterKey);
    }

    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return ledgerIndex.readMasterKey(ledgerId);
    }

    static final String getLedgerFilename(long ledgerId) {
        int parent = (int) (ledgerId & 0xff);
        int grandParent = (int) ((ledgerId & 0xff00) >> 8);
        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toHexString(grandParent));
        sb.append('/');
        sb.append(Integer.toHexString(parent));
        sb.append('/');
        sb.append(Long.toHexString(ledgerId));
        sb.append(".ldgr");
        return sb.toString();
    }

    private File findLedgerFile(long ledgerId) throws IOException {
        String ledgerName = getLedgerFilename(ledgerId);
        for(File d: ledgerDirectories) {
            File lf = new File(d, ledgerName);
            if (lf.exists()) {
                return lf;
            }
        }
        return null;
    }

    static final private Random rand = new Random();

    static final private File pickDirs(File dirs[]) {
        return dirs[rand.nextInt(dirs.length)];
    }

    static final public ByteBuffer signature = ByteBuffer.wrap("BKLD".getBytes());

    synchronized LedgerFile createLedgerFile(long ledgerId) throws IOException {
        File lf = findLedgerFile(ledgerId);
        if (lf == null) {
            File dir = pickDirs(ledgerDirectories);
            String ledgerName = getLedgerFilename(ledgerId);
            lf = new File(dir, ledgerName);
            if (!lf.getParentFile().exists()) {
                lf.getParentFile().mkdirs();
            }
        }
        
        FileChannel fc = new RandomAccessFile(lf, "rw").getChannel();
        LedgerFile f = new LedgerFile(fc);
        if (fc.size() == 0) {
            signature.clear();
            fc.write(signature);
            f.markDirty();
        } else {
            fc.position(fc.size());
        }
        return f;
    }

    public long addEntry(ByteBuffer entry) throws IOException {
        ByteBuffer offsetBuffer = ByteBuffer.wrap(new byte[8]);
        long ledgerId = entry.getLong();

        LedgerFile lf;
        try {
            lf = fileCache.get(ledgerId);
        } catch (ExecutionException ee) {
            throw new IOException("Couldn't get file", ee);
        }
        FileChannel ledger = lf.getLedgerFile();
        lf.markDirty();
        somethingWritten = true;    
        /*
         * Get entry id
         */
            
        long entryId = entry.getLong();
        entry.rewind();
            
        long pos = ledger.position();
        LOG.debug("Putting entry offset for {}:{} = {}", new Object[] {ledgerId, entryId, pos});

        ledgerIndex.putEntryOffset(ledgerId, entryId, pos);
            
        ByteBuffer lenBuffer = ByteBuffer.allocate(4);        
        lenBuffer.putInt(entry.remaining());
        lenBuffer.flip();
            
        /*
         * Write length of entry first, then the entry itself
         */
        ledger.write(lenBuffer);
        ledger.write(entry);
        
        return entryId;
    }

    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        FileChannel ledger;
        try {
            ledger = fileCache.get(ledgerId).getLedgerFile();
        } catch (ExecutionException ee) {
            throw new IOException("Couldn't get file", ee);
        }

        /*
         * If entryId is -1, then return the last written.
         */
        if (entryId == -1) {
            entryId = ledgerIndex.getLastEntry(ledgerId);
        }
        long offset = ledgerIndex.getEntryOffset(ledgerId, entryId);
        LOG.debug("Entry offset for {}:{} = {}", new Object[] {ledgerId, entryId, offset});
        if (offset == 0) {
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }
        ByteBuffer buffer = ByteBuffer.wrap(new byte[4]);
        buffer.limit(4);
        buffer.rewind();
        /*
         * Read the length
         */
        ledger.read(buffer, offset);
        buffer.flip();
        int len = buffer.getInt();
        buffer = ByteBuffer.allocate(len);
        /*
         * Read the rest. We add 4 to skip the length
         */
        ledger.read(buffer, offset + 4);
        buffer.flip();
        return buffer;
    }

    public boolean isFlushRequired() {
        return somethingWritten;
    }

    synchronized public void flush() throws IOException {
        if (!somethingWritten) {
            return;
        }

        somethingWritten = false;
        boolean flushFailed = false;
        
        try {
            ledgerIndex.flushLedger(true);
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger cache", ioe);
            flushFailed = true;
        }
        
        try {
            for (Map.Entry<Long,LedgerFile> e : fileCache.asMap().entrySet()) {
                e.getValue().sync();
            }
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger", ioe);
            flushFailed = true;
        }
        if (flushFailed) {
            throw new IOException("Flushing to storage failed, check logs");
        }
    }

    public BKMBeanInfo getJMXBean() {
        return ledgerIndex.getJMXBean();
    }

    static private final long calcEntryOffset(long entryId) {
        return 8L*entryId;
    }
}