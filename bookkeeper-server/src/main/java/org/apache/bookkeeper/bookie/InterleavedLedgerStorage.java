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


class InterleavedLedgerStorage implements LedgerStorage {
    private EntryLogger entryLogger;
    private LedgerCache ledgerCache;
    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    final GarbageCollectorThread gcThread;

    InterleavedLedgerStorage(ServerConfiguration conf, LedgerManager lm) {
        entryLogger = new EntryLogger(conf);
        ledgerCache = new LedgerCacheImpl(conf, ledgerManager);
        gcThread = new GarbageCollectorThread(conf, this.zk, ledgerCache, entryLogger,
                ledgerManager, new EntryLogCompactionScanner());
    }

    @Override    
    void start() {
        gcThread.start();
    }

    @Override
    void shutdown() {
        // shut down gc thread, which depends on zookeeper client
        // also compaction will write entries again to entry log file
        gcThread.shutdown();
        entryLogger.shutdown();
    }

    @Override
    void setMasterKey(long ledgerId, byte[] masterKey) {
        ledgerCache.setMasterKey(ledgerId, masterKey);
    }

    @Override
    byte[] readMasterKey(long ledgerId) {
        return ledgerCache.readMasterKey(ledgerId);
    }

    @Override
    boolean ledgerExists(long ledgerId) {
        return ledgerCache.ledgerExists(ledgerId);
    }

    @Override
    void addEntry(ByteBuffer entry) {
    }

    @Override
    ByteBuffer getEntry(long ledgerId, long entryId) {
    }

    @Override
    boolean isFlushRequired() {
        entryLogger.testAndClearSomethingWritten()
    };

    @Override
    void flush() throws IOException {
        boolean flushFailed = false;
        try {
            ledgerCache.flushLedger(true);
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger cache", e);
            flushFailed = true;
        }

        try {
            entryLogger.flush();
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger", e);
            flushFailed = true;
        }
    }
}