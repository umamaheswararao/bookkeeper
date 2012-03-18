package org.apache.bookkeeper.bookie;

import java.io.IOException;

interface HandleRegistry {
    LedgerDescriptor getHandle(long ledgerId, byte[] masterKey)
            throws IOException, BookieException;

    LedgerDescriptor getReadOnlyHandle(long ledgerId)
            throws IOException, Bookie.NoLedgerException;

    void releaseHandle(LedgerDescriptor handle);
}