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

package org.apache.bookkeeper.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Encapsulates asynchronous ledger create operation
 *
 */
class LedgerCreateOp implements GenericCallback<String> {

    static final Logger LOG = LoggerFactory.getLogger(LedgerCreateOp.class);

    CreateCallback cb;
    LedgerMetadata metadata;
    LedgerHandle lh;
    Object ctx;
    byte[] passwd;
    BookKeeper bk;
    DigestType digestType;

    /**
     * Constructor
     *
     * @param bk
     *       BookKeeper object
     * @param ensembleSize
     *       ensemble size
     * @param quorumSize
     *       quorum size
     * @param digestType
     *       digest type, either MAC or CRC32
     * @param passwd
     *       passowrd
     * @param cb
     *       callback implementation
     * @param ctx
     *       optional control object
     */

    LedgerCreateOp(BookKeeper bk, int ensembleSize,
                   int writeQuorumSize, int ackQuorumSize,
                   DigestType digestType,
                   byte[] passwd, CreateCallback cb, Object ctx) {
        this.bk = bk;
        this.metadata = new LedgerMetadata(ensembleSize, writeQuorumSize, ackQuorumSize);
        this.digestType = digestType;
        this.passwd = passwd;
        this.cb = cb;
        this.ctx = ctx;
    }

    /**
     * Initiates the operation
     */
    public void initiate() {
        // allocate ensemble first

        /*
         * Adding bookies to ledger handle
         */

        ArrayList<InetSocketAddress> ensemble;
        try {
            ensemble = bk.bookieWatcher.getNewBookies(metadata.ensembleSize);
        } catch (BKNotEnoughBookiesException e) {
            LOG.error("Not enough bookies to create ledger");
            cb.createComplete(e.getCode(), null, this.ctx);
            return;
        }

        /*
         * Add ensemble to the configuration
         */
        metadata.addEnsemble(new Long(0), ensemble);

        // create a ledger path with metadata
        bk.getLedgerManager().newLedgerPath(this, metadata);
    }

    /**
     * Callback when created ledger path.
     */
    @Override
    public void operationComplete(int rc, String ledgerPath) {

        if (rc != KeeperException.Code.OK.intValue()) {
            LOG.error("Could not create node for ledger",
                      KeeperException.create(KeeperException.Code.get(rc), ledgerPath));
            cb.createComplete(BKException.Code.ZKException, null, this.ctx);
            return;
        }

        /*
         * Extract ledger id.
         */
        long ledgerId;
        try {
            ledgerId = bk.getLedgerManager().getLedgerId(ledgerPath);
        } catch (IOException e) {
            LOG.error("Could not extract ledger-id from path:" + ledgerPath, e);
            cb.createComplete(BKException.Code.ZKException, null, this.ctx);
            return;
        }

        try {
            lh = new LedgerHandle(bk, ledgerId, metadata, digestType, passwd);
        } catch (GeneralSecurityException e) {
            LOG.error("Security exception while creating ledger: " + ledgerId, e);
            cb.createComplete(BKException.Code.DigestNotInitializedException, null, this.ctx);
            return;
        } catch (NumberFormatException e) {
            LOG.error("Incorrectly entered parameter throttle: " + bk.getConf().getThrottleValue(), e);
            cb.createComplete(BKException.Code.IncorrectParameterException, null, this.ctx);
            return;
        }

        // return the ledger handle back
        cb.createComplete(BKException.Code.OK, lh, this.ctx);
    }

}
