package org.apache.bookkeeper.bookie.HandleRegistry;

import java.io.IOException;
import java.util.HashMap;

public class HandleRegistryImpl implements HandleRegistry {
    HashMap<Long, LedgerDescriptor> ledgers = new HashMap<Long, LedgerDescriptor>();
    HashMap<Long, ReadOnlyLedgerDescriptor> readOnlyLedgers
        = new HashMap<Long, ReadOnlyLedgerDescriptor>();

    final EntryLogger entryLogger;
    final LedgerCache ledgerCache;

    HandleRegistryImpl(EntryLogger entryLogger, LedgerCache ledgerCache) {
        this.entryLogger = entryLogger;
        this.ledgerCache = ledgerCache;
    }

    LedgerDescriptor getHandle(long ledgerId) {
        // load handle
    }

    LedgerDescriptor getHandle(long ledgerId, byte[] masterKey)
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

    LedgerDescriptor getReadOnlyHandle(long ledgerId)
            throws IOException, BookieException {
        LedgerDescriptor handle = null;
        synchronized (ledgers) {
            handle = readOnlyLedgers.get(ledgerId);
            if (handle == null) {
                handle = LedgerDescriptor.createReadOnly(ledgerId, entryLogger, ledgerCache);
                readOnlyledgers.put(ledgerId, handle);
            }
            handle.incRef();
        }
        return handle;
    }

    void releaseHandle(LedgerDescriptor handle) {
        synchronized (ledgers) {
            handle.decRef();
        }
    }
}