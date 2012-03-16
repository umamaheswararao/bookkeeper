package org.apache.bookkeeper.bookie.HandleRegistry;

public interface HandleRegistry {
    LedgerDescriptor getHandle(long ledgerId, byte[] masterKey)
            throws IOException, BookieException;

    LedgerDescriptor getReadOnlyHandle(long ledgerId)
            throws IOException, BookieException;

    void releaseHandle(LedgerDescriptor handle);
}