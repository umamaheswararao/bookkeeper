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

import java.nio.ByteBuffer;
import java.io.IOException;

import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interleave ledger storage
 * This ledger storage implementation stores all entries in a single
 * file and maintains an index file for each ledger.
 */
class InterleavedLedgerStorage implements LedgerStorage {
    final static Logger LOG = LoggerFactory.getLogger(InterleavedLedgerStorage.class);

    private EntryLogger entryLogger;
    private LedgerCache ledgerCache;
    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    final GarbageCollectorThread gcThread;
    final Object flushLock = new Object();

    // this indicates that a write has happened since the last flush
    private volatile boolean somethingWritten = false;

    InterleavedLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager)
            throws IOException {
        entryLogger = new EntryLogger(conf);
        ledgerCache = new LedgerCacheImpl(conf, ledgerManager);
        gcThread = new GarbageCollectorThread(conf, ledgerCache, entryLogger,
                ledgerManager, new EntryLogCompactionScanner());
    }

    @Override    
    public void start() {
        gcThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        // shut down gc thread, which depends on zookeeper client
        // also compaction will write entries again to entry log file
        gcThread.shutdown();
        entryLogger.shutdown();
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        ledgerCache.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return ledgerCache.readMasterKey(ledgerId);
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return ledgerCache.ledgerExists(ledgerId);
    }

    @Override
    synchronized public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();
        
        /*
         * Log the entry
         */
        long pos = entryLogger.addEntry(ledgerId, entry);
        
        
        /*
         * Set offset of entry id to be the current ledger position
         */
        ledgerCache.putEntryOffset(ledgerId, entryId, pos);

        somethingWritten = true;

        return entryId;
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        long offset;
        /*
         * If entryId is -1, then return the last written.
         */
        if (entryId == -1) {
            entryId = ledgerCache.getLastEntry(ledgerId);
        }

        offset = ledgerCache.getEntryOffset(ledgerId, entryId);
        if (offset == 0) {
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }
        return ByteBuffer.wrap(entryLogger.readEntry(ledgerId, entryId, offset));
    }

    @Override
    public boolean isFlushRequired() {
        return somethingWritten;
    };

    @Override
    public void flush() throws IOException {
        synchronized (flushLock) {
            if (!somethingWritten) {
                return;
            }
            somethingWritten = false;
            boolean flushFailed = false;

            try {
                ledgerCache.flushLedger(true);
            } catch (IOException ioe) {
                LOG.error("Exception flushing Ledger cache", ioe);
                flushFailed = true;
            }
            
            try {
                entryLogger.flush();
            } catch (IOException ioe) {
                LOG.error("Exception flushing Ledger", ioe);
                flushFailed = true;
            }
            if (flushFailed) {
                throw new IOException("Flushing to storage failed, check logs");
            }
        }
    }

    @Override
    public BKMBeanInfo getJMXBean() {
        return ledgerCache.getJMXBean();
    }

    /**
     * Scanner used to do entry log compaction
     */
    class EntryLogCompactionScanner implements EntryLogger.EntryLogScanner {
        @Override
        public boolean accept(long ledgerId) {
            // bookie has no knowledge about which ledger is deleted
            // so just accept all ledgers.
            return true;
        }

        @Override
        public void process(long ledgerId, ByteBuffer buffer)
            throws IOException {
            addEntry(buffer);
        }
    }

}