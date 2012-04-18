package org.apache.bookkeeper.benchmark;
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


import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

/**
 * This is a simple test program to compare the performance of writing to
 * BookKeeper and to the local file system.
 *
 */

public class TestClient
    implements AddCallback, ReadCallback {
    private static final Logger LOG = LoggerFactory.getLogger(TestClient.class);

    BookKeeper x;
    LedgerHandle lh;
    Integer entryId;
    HashMap<Integer, Integer> map;

    OutputStream fStream;
    FileOutputStream fStreamLocal;
    long start, lastId;

    public TestClient() {
        entryId = 0;
        map = new HashMap<Integer, Integer>();
    }

    public TestClient(String servers) throws KeeperException, IOException, InterruptedException {
        this();
        x = new BookKeeper(servers);
        try {
            lh = x.createLedger(DigestType.CRC32, new byte[] {'a', 'b'});
        } catch (BKException e) {
            LOG.error(e.toString());
        }
    }

    public TestClient(String servers, int ensSize, int qSize)
            throws KeeperException, IOException, InterruptedException {
        this();
        x = new BookKeeper(servers);
        try {
            lh = x.createLedger(ensSize, qSize, DigestType.CRC32, new byte[] {'a', 'b'});
        } catch (BKException e) {
            LOG.error(e.toString());
        }
    }

    public TestClient(OutputStream fStream)
            throws FileNotFoundException {
        this.fStream = fStream;
        this.fStreamLocal = new FileOutputStream("./local.log");
    }


    public Integer getFreshEntryId(int val) {
        ++this.entryId;
        synchronized (map) {
            map.put(this.entryId, val);
        }
        return this.entryId;
    }

    public boolean removeEntryId(Integer id) {
        boolean retVal = false;
        synchronized (map) {
            map.remove(id);
            retVal = true;

            if(map.size() == 0) map.notifyAll();
            else {
                if(map.size() < 4)
                    LOG.error(map.toString());
            }
        }
        return retVal;
    }

    public void closeHandle() throws KeeperException, InterruptedException, BKException {
        lh.close();
        x.close();
    }
    /**
     * First says if entries should be written to BookKeeper (0) or to the local
     * disk (1). Second parameter is an integer defining the length of a ledger entry.
     * Third parameter is the number of writes.
     *
     * @param args
     */
    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("length", true, "Length of packets being written. Default 1024");
        options.addOption("target", true, "Target medium to write to. Options are bk, fs & hdfs. Default fs");
        options.addOption("count", true, "Number of packets to write in run. Default 10000");
        options.addOption("path", true, "Path to write to. fs & hdfs only. Default /foobar");
        options.addOption("zkservers", true, "ZooKeeper servers, comma separated. bk only. Default localhost:2181.");
        options.addOption("bkensemble", true, "BookKeeper ledger ensemble size. bk only. Default 3");
        options.addOption("bkquorum", true, "BookKeeper ledger quorum size. bk only. Default 2");
        options.addOption("sync", false, "Use synchronous writes with BookKeeper. bk only.");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TestClient <options>", options);
            System.exit(-1);
        }

        int length = Integer.valueOf(cmd.getOptionValue("length", "1024"));
        String target = cmd.getOptionValue("target", "fs");
        int count = Integer.valueOf(cmd.getOptionValue("count", "10000"));
        String path = cmd.getOptionValue("path", "/foobar");
        String zkservers = cmd.getOptionValue("zkservers", "localhost:2181");
        int bkensemble = Integer.valueOf(cmd.getOptionValue("bkensemble", "3"));
        int bkquorum = Integer.valueOf(cmd.getOptionValue("bkquorum", "2"));

        StringBuilder sb = new StringBuilder();
        while(length-- > 0) {
            sb.append('a');
        }

        if (target.equals("bk")) {
            try {
                TestClient c = new TestClient(zkservers, bkensemble, bkquorum);
                if (cmd.hasOption("sync")) {
                    c.writeSameEntryBatchSync(sb.toString().getBytes(), count);
                } else {
                    c.writeSameEntryBatch(sb.toString().getBytes(), count);
                }
                //c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                c.closeHandle();
            } catch (Exception e) {
                LOG.error("Exception occurred", e);
            } 
        } else if (target.equals("fs")) {
            try {
                TestClient c = new TestClient(new FileOutputStream(path));
                c.writeSameEntryBatchFS(sb.toString().getBytes(), count);
            } catch(FileNotFoundException e) {
                LOG.error("File not found", e);
            }
        } else if (target.equals("hdfs")) {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                LOG.info("Default replication for HDFS: {}", fs.getDefaultReplication());
                TestClient c = new TestClient(fs.create(new Path(path)));
                c.writeSameEntryBatchFS(sb.toString().getBytes(), count);
            } catch(IOException e) {
                LOG.error("File not found", e);
            }
        } else {
            System.err.println("Unknown option: " + target);
            System.exit(2);
        }
    }

    void writeSameEntryBatch(byte[] data, int times) throws InterruptedException {
        start = System.currentTimeMillis();
        int count = times;
        LOG.debug("Data: " + new String(data) + ", " + data.length);
        while(count-- > 0) {
            lh.asyncAddEntry(data, this, this.getFreshEntryId(2));
        }
        LOG.info("Finished " + times + " async writes in ms: " + (System.currentTimeMillis() - start));
        synchronized (map) {
            if(map.size() != 0)
                map.wait();
        }
        LOG.info("Finished processing in ms: " + (System.currentTimeMillis() - start));

        LOG.debug("Ended computation");
    }

    void writeSameEntryBatchSync(byte[] data, int times) throws InterruptedException, BKException {
        start = System.currentTimeMillis();
        int count = times;
        LOG.debug("Data: " + new String(data) + ", " + data.length);
        while(count-- > 0) {
            lh.addEntry(data);
        }
        LOG.info("Finished processing in ms: " + (System.currentTimeMillis() - start));

        LOG.debug("Ended computation");
    }

    void writeConsecutiveEntriesBatch(int times) throws InterruptedException {
        start = System.currentTimeMillis();
        int count = times;
        while(count-- > 0) {
            byte[] write = new byte[2];
            int j = count%100;
            int k = (count+1)%100;
            write[0] = (byte) j;
            write[1] = (byte) k;
            lh.asyncAddEntry(write, this, this.getFreshEntryId(2));
        }
        LOG.info("Finished " + times + " async writes in ms: " + (System.currentTimeMillis() - start));
        synchronized (map) {
            if(map.size() != 0)
                map.wait();
        }
        LOG.info("Finished processing writes (ms): " + (System.currentTimeMillis() - start));

        Integer mon = Integer.valueOf(0);
        synchronized(mon) {
            lh.asyncReadEntries(1, times - 1, this, mon);
            mon.wait();
        }
        LOG.error("Ended computation");
    }

    void writeSameEntryBatchFS(byte[] data, int times) {
        int count = times;
        LOG.debug("Data: " + data.length + ", " + times);
        try {
            start = System.currentTimeMillis();
            while(count-- > 0) {
                fStream.write(data);
                //fStreamLocal.write(data);
                fStream.flush();
                if (fStream instanceof FSDataOutputStream) {
                    ((FSDataOutputStream)fStream).hflush();
                } else if (fStream instanceof FileOutputStream) {
                    ((FileOutputStream)fStream).getChannel().force(false);
                }
            }
            fStream.close();
            LOG.info("Finished processing writes (ms): " + (System.currentTimeMillis() - start));
        } catch(IOException e) {
            LOG.error("IOException occurred", e);
        }
    }


    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        this.removeEntryId((Integer) ctx);
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
        System.out.println("Read callback: " + rc);
        while(seq.hasMoreElements()) {
            LedgerEntry le = seq.nextElement();
            LOG.debug(new String(le.getEntry()));
        }
        synchronized(ctx) {
            ctx.notify();
        }
    }
}
