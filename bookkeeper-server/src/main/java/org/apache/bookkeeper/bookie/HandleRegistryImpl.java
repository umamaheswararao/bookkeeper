package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.util.HashMap;

class HandleRegistryImpl implements HandleRegistry {
    HashMap<Long, LedgerDescriptor> ledgers = new HashMap<Long, LedgerDescriptor>();
    HashMap<Long, LedgerDescriptor> readOnlyLedgers
        = new HashMap<Long, LedgerDescriptor>();

    final EntryLogger entryLogger;
    final LedgerCache ledgerCache;

    HandleRegistryImpl(EntryLogger entryLogger, LedgerCache ledgerCache) {
        this.entryLogger = entryLogger;
        this.ledgerCache = ledgerCache;
    }

    @Override
    public LedgerDescriptor getHandle(long ledgerId, byte[] masterKey)
            throws IOException, BookieException {
        LedgerDescriptor handle = null;
        synchronized (ledgers) {
            handle = ledgers.get(ledgerId);
            if (handle == null) {
                handle = LedgerDescriptor.create(masterKey, ledgerId,
                                                 entryLogger, ledgerCache);
                ledgers.put(ledgerId, handle);
            }
            handle.checkAccess(masterKey);
            handle.incRef();
        }
        return handle;
    }

    @Override
    public LedgerDescriptor getReadOnlyHandle(long ledgerId)
            throws IOException, Bookie.NoLedgerException {
        LedgerDescriptor handle = null;
        synchronized (ledgers) {
            handle = readOnlyLedgers.get(ledgerId);
            if (handle == null) {
                handle = LedgerDescriptor.createReadOnly(ledgerId, entryLogger, ledgerCache);
                readOnlyLedgers.put(ledgerId, handle);
            }
            handle.incRef();
        }
        return handle;
    }

    @Override
    public void releaseHandle(LedgerDescriptor handle) {
        synchronized (ledgers) {
            handle.decRef();
        }
    }
}