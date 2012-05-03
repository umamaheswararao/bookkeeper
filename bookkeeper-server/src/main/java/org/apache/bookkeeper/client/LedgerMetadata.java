package org.apache.bookkeeper.client;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.bookkeeper.util.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates all the ledger metadata that is persistently stored
 * in zookeeper. It provides parsing and serialization methods of such metadata.
 *
 */
public class LedgerMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LedgerMetadata.class);

    private static final String closed = "CLOSED";
    private static final String lSplitter = "\n";
    private static final String tSplitter = "\t";

    // can't use -1 for NOTCLOSED because that is reserved for a closed, empty
    // ledger
    public static final int NOTCLOSED = -101;
    public static final int IN_RECOVERY = -102;

    public static final int LOWEST_COMPAT_METADATA_FORMAT_VERSION = 0;
    public static final int CURRENT_METADATA_FORMAT_VERSION = 2;
    public static final String VERSION_KEY = "BookieMetadataFormatVersion";

    int metadataFormatVersion = 0;

    int ensembleSize;
    int writeQuorumSize;
    int ackQuorumSize;
    long length;
    long close;
    private SortedMap<Long, ArrayList<InetSocketAddress>> ensembles = new TreeMap<Long, ArrayList<InetSocketAddress>>();
    ArrayList<InetSocketAddress> currentEnsemble;
    volatile int znodeVersion = -1;
    
    public LedgerMetadata(int ensembleSize, int writeQuorumSize, int ackQuorumSize) {
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;

        /*
         * It is set in PendingReadOp.readEntryComplete, and
         * we read it in LedgerRecoveryOp.readComplete.
         */
        this.length = 0;
        this.close = NOTCLOSED;
        this.metadataFormatVersion = CURRENT_METADATA_FORMAT_VERSION;
    };

    private LedgerMetadata() {
        this(0, 0, 0);
    }

    /**
     * Get the Map of bookie ensembles for the various ledger fragments
     * that make up the ledger.
     *
     * @return SortedMap of Ledger Fragments and the corresponding
     * bookie ensembles that store the entries.
     */
    public SortedMap<Long, ArrayList<InetSocketAddress>> getEnsembles() {
        return ensembles;
    }

    boolean isClosed() {
        return close != NOTCLOSED 
            && close != IN_RECOVERY;
    }

    boolean isInRecovery() {
        return IN_RECOVERY == close;
    }
    
    void markLedgerInRecovery() {
        close = IN_RECOVERY;
    }

    void close(long entryId) {
        close = entryId;
    }

    void addEnsemble(long startEntryId, ArrayList<InetSocketAddress> ensemble) {
        assert ensembles.isEmpty() || startEntryId >= ensembles.lastKey();

        ensembles.put(startEntryId, ensemble);
        currentEnsemble = ensemble;
    }

    ArrayList<InetSocketAddress> getEnsemble(long entryId) {
        // the head map cannot be empty, since we insert an ensemble for
        // entry-id 0, right when we start
        return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
    }

    /**
     * the entry id > the given entry-id at which the next ensemble change takes
     * place ( -1 if no further ensemble changes)
     *
     * @param entryId
     * @return
     */
    long getNextEnsembleChange(long entryId) {
        SortedMap<Long, ArrayList<InetSocketAddress>> tailMap = ensembles.tailMap(entryId + 1);

        if (tailMap.isEmpty()) {
            return -1;
        } else {
            return tailMap.firstKey();
        }
    }

    /**
     * Generates a byte array of this object
     *
     * @return the metadata serialized into a byte array
     */
    public byte[] serialize() {
        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(tSplitter).append(metadataFormatVersion).append(lSplitter);
        s.append(writeQuorumSize).append(lSplitter).append(ackQuorumSize).append(lSplitter)
            .append(ensembleSize).append(lSplitter).append(length);

        for (Map.Entry<Long, ArrayList<InetSocketAddress>> entry : ensembles.entrySet()) {
            s.append(lSplitter).append(entry.getKey());
            for (InetSocketAddress addr : entry.getValue()) {
                s.append(tSplitter);
                StringUtils.addrToString(s, addr);
            }
        }

        if (close != NOTCLOSED) {
            s.append(lSplitter).append(close).append(tSplitter).append(closed);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Serialized config: " + s.toString());
        }

        return s.toString().getBytes();
    }

    /**
     * Parses a given byte array and transforms into a LedgerConfig object
     *
     * @param array
     *            byte array to parse
     * @return LedgerConfig
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */

    static LedgerMetadata parseConfig(byte[] bytes, int version) throws IOException {

        LedgerMetadata lc = new LedgerMetadata();
        String config = new String(bytes);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parsing Config: " + config);
        }
        
        String lines[] = config.split(lSplitter);
        
        try {
            int i = 0;
            if (lines[0].startsWith(VERSION_KEY)) {
                String parts[] = lines[0].split(tSplitter);
                lc.metadataFormatVersion = new Integer(parts[1]);
                i++;
            } else {
                lc.metadataFormatVersion = 0;
            }
            
            if (lc.metadataFormatVersion < LOWEST_COMPAT_METADATA_FORMAT_VERSION
                || lc.metadataFormatVersion > CURRENT_METADATA_FORMAT_VERSION) {
                throw new IOException("Metadata version not compatible. Expected between "
                        + LOWEST_COMPAT_METADATA_FORMAT_VERSION + " and " + CURRENT_METADATA_FORMAT_VERSION
                        + ", but got " + lc.metadataFormatVersion);
            }
            if ((lines.length+i) < 2) {
                throw new IOException("Quorum size or ensemble size absent from config: " + config);
            }

            lc.znodeVersion = version;
            lc.writeQuorumSize = new Integer(lines[i++]);
            if (lc.metadataFormatVersion >= 2) {
                lc.ackQuorumSize = new Integer(lines[i++]);
            } else {
                lc.ackQuorumSize = lc.writeQuorumSize;
            }
            lc.ensembleSize = new Integer(lines[i++]);
            lc.length = new Long(lines[i++]);

            for (; i < lines.length; i++) {
                String parts[] = lines[i].split(tSplitter);

                if (parts[1].equals(closed)) {
                    lc.close = new Long(parts[0]);
                    break;
                }

                ArrayList<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
                for (int j = 1; j < parts.length; j++) {
                    addrs.add(StringUtils.parseAddr(parts[j]));
                }
                lc.addEnsemble(new Long(parts[0]), addrs);
            }
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
        return lc;
    }
    

    /**
     * Updates the status of this metadata in ZooKeeper.
     * 
     * @param stat
     */
    public void updateZnodeStatus(Stat stat) {
        this.znodeVersion = stat.getVersion();
    }

    /**
     * Update the znode version of this metadata
     *
     * @param znodeVersion
     *        Znode version of this metadata
     */
    public void updateZnodeStatus(int znodeVersion) {
        this.znodeVersion = znodeVersion;
    }

    /**
     * Returns the last znode version.
     * 
     * @return int znode version
     */
    public int getZnodeVersion() {
        return this.znodeVersion;
    }

    /**
     * Resolve conflict with new updated metadata.
     *
     * @param newMeta
     *          Re-read metadata
     * @return true if the conflict has been resolved, otherwise false.
     */
    boolean resolveConflict(LedgerMetadata newMeta) {
        /*
         *  if length & close have changed, then another client has
         *  opened the ledger, can't resolve this conflict.
         */

        if (metadataFormatVersion != newMeta.metadataFormatVersion ||
            ensembleSize != newMeta.ensembleSize ||
            writeQuorumSize != newMeta.writeQuorumSize ||
            ackQuorumSize != newMeta.ackQuorumSize ||
            length != newMeta.length ||
            close != newMeta.close) {
            return false;
        }
        // new meta znode version should be larger than old one
        if (znodeVersion > newMeta.znodeVersion) {
            return false;
        }
        // ensemble size should be same
        if (ensembles.size() != newMeta.ensembles.size()) {
            return false;
        }
        // ensemble distribution should be same
        // we don't check the detail ensemble, since new bookie will be set
        // using recovery tool.
        Iterator<Long> keyIter = ensembles.keySet().iterator();
        Iterator<Long> newMetaKeyIter = newMeta.ensembles.keySet().iterator();
        for (int i=0; i<ensembles.size(); i++) {
            Long curKey = keyIter.next();
            Long newMetaKey = newMetaKeyIter.next();
            if (curKey != newMetaKey) {
                return false;
            }
        }
        /*
         *  if the conflict has been resolved, then update
         *  ensemble and znode version
         */
        ensembles = newMeta.ensembles;
        znodeVersion = newMeta.znodeVersion;
        return true;
    }
}
