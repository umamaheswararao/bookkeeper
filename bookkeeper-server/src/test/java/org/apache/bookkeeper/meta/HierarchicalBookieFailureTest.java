package org.apache.bookkeeper.meta;

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

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.meta.HierarchicalLedgerManager;
import org.apache.bookkeeper.test.BookieFailureTest;

import org.junit.Before;

/**
 * Test Bookie Failure using HierarchicalLedgerManager
 */
public class HierarchicalBookieFailureTest extends BookieFailureTest {

    public HierarchicalBookieFailureTest(DigestType digestType) {
        super(digestType);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        baseConf.setLedgerManagerType(HierarchicalLedgerManager.NAME);
        baseClientConf.setLedgerManagerType(HierarchicalLedgerManager.NAME);
        super.setUp();
    }

}
