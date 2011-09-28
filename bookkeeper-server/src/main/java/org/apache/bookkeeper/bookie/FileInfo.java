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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

/**
 * This is the file handle for a ledger's index file that maps entry ids to location.
 * It is used by LedgerCache.
 */
class FileInfo {
    static Logger LOG = Logger.getLogger(FileInfo.class);

    private FileChannel fc;
    private final File lf;
    byte headerData[];
    /**
     * The fingerprint of a ledger index file. the next four bytes will be the version followed by four bytes for the length of the header structure.
     */
    static final public int signature = ByteBuffer.wrap("BKLE".getBytes()).getInt();
    static final public int headerVersion = 1;
    
    static final long START_OF_DATA = 1024;
    private long size;
    private int useCount;
    private boolean isClosed;
    public FileInfo(File lf, byte headerData[]) {
        this.lf = lf;
        this.headerData = headerData;
    }

    private void checkOpen(boolean create) throws IOException {
        if (fc != null) {
            return;
        }
        boolean exists = lf.exists();
        if (headerData == null && !exists) {
            throw new IOException(lf + " not found");
        }
        ByteBuffer bb = ByteBuffer.allocate(1024);
        if (!exists) { 
            if (create) {
                fc = new RandomAccessFile(lf, "rw").getChannel();
                size = fc.size();
                if (size == 0) {
                    bb.putInt(signature);
                    bb.putInt(headerVersion);
                    bb.putInt(headerData.length);
                    bb.put(headerData);
                    bb.rewind();
                    fc.write(bb);
                }
            }
        } else {
            fc = new RandomAccessFile(lf, "rw").getChannel();
            size = fc.size();

            while(bb.hasRemaining()) {
                fc.read(bb);
            }
            bb.flip();
            if (bb.getInt() != signature) {
                throw new IOException("Missing ledger signature");
            }
            int version = bb.getInt();
            if (version != headerVersion) {
                throw new IOException("Incompatible ledger version " + version);
            }
            int length = bb.getInt();
            if (length < 0 || length > bb.remaining()) {
                throw new IOException("Length " + length + " is invalid");
            }
            headerData = new byte[length];
            bb.get(headerData);
        }
    }
    synchronized public long size() throws IOException {
        checkOpen(false);
        long rc = size-START_OF_DATA;
        if (rc < 0) {
            rc = 0;
        }
        return rc;
    }
    
    synchronized public int read(ByteBuffer bb, long position) throws IOException {
        checkOpen(false);
        int total = 0;
        while(bb.remaining() > 0) {
            int rc = fc.read(bb, position+START_OF_DATA);
            if (rc <= 0) {
                throw new IOException("Short read");
            }
            total += rc;
        }
        return total;
    }

    synchronized public void close() throws IOException {
        isClosed = true;
        if (useCount == 0 && fc != null) {
            fc.close();
        }
    }

    synchronized public long write(ByteBuffer[] buffs, long position) throws IOException {
        checkOpen(true);
        long total = 0;
        try {
            fc.position(position+START_OF_DATA);
            while(buffs[buffs.length-1].remaining() > 0) {
                long rc = fc.write(buffs);
                if (rc <= 0) {
                    throw new IOException("Short write");
                }
                total += rc;
            }
        } finally {
	    fc.force(true);
            long newsize = position+START_OF_DATA+total;
            if (newsize > size) {
                size = newsize;
            }
        }
        return total;
    }
    
    synchronized public byte[] getHeaderData() throws IOException {
        checkOpen(false);
        return headerData;
    }

    synchronized public void use() {
        useCount++;
    }

    synchronized public void release() {
        useCount--;
        if (isClosed && useCount == 0 && fc != null) {
            try {
                fc.close();
            } catch (IOException e) {
                LOG.error("Error closing file channel", e);
            }
        }
    }

    public boolean delete() {
        return lf.delete();
    }

}
