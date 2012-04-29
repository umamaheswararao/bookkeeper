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
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import java.util.concurrent.Future;;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
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

    public TestClient(String servers, int ensSize, int qSize, int throttle)
            throws KeeperException, IOException, InterruptedException {
        this();
        ClientConfiguration conf = new ClientConfiguration();
        conf.setThrottleValue(100000);
        conf.setZkServers(servers);

        x = new BookKeeper(conf);
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
        options.addOption("runfor", true, "Number of seconds to run for. Default 60");
        options.addOption("path", true, "Path to write to. fs & hdfs only. Default /foobar");
        options.addOption("zkservers", true, "ZooKeeper servers, comma separated. bk only. Default localhost:2181.");
        options.addOption("bkensemble", true, "BookKeeper ledger ensemble size. bk only. Default 3");
        options.addOption("bkquorum", true, "BookKeeper ledger quorum size. bk only. Default 2");
        options.addOption("bkthrottle", true, "BookKeeper throttle size. bk only. Default 10000");
        options.addOption("sync", false, "Use synchronous writes with BookKeeper. bk only.");
        options.addOption("numfiles", true, "Number of files to write to concurrently. hdfs only");
        options.addOption("timeout", true, "Number of seconds after which to give up");
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
        long runfor = Long.valueOf(cmd.getOptionValue("runfor", "60")) * 1000;

        StringBuilder sb = new StringBuilder();
        while(length-- > 0) {
            sb.append('a');
        }

        Timer timeouter = new Timer();
        if (cmd.hasOption("timeout")) {
            final long timeout = Long.valueOf(cmd.getOptionValue("timeout", "360")) * 1000;

            timeouter.schedule(new TimerTask() {
                    public void run() {
                        System.err.println("Timing out benchmark after " + timeout + "ms");
                        System.exit(-1);
                    }
                }, timeout);
        }
        if (target.equals("bk")) {
            try {
                String zkservers = cmd.getOptionValue("zkservers", "localhost:2181");
                int bkensemble = Integer.valueOf(cmd.getOptionValue("bkensemble", "3"));
                int bkquorum = Integer.valueOf(cmd.getOptionValue("bkquorum", "2"));
                int bkthrottle = Integer.valueOf(cmd.getOptionValue("bkthrottle", "10000"));
                TestClient c = new TestClient(zkservers, bkensemble, bkquorum, bkthrottle);
                if (cmd.hasOption("sync")) {
                    //c.writeSameEntryBatchSync(sb.toString().getBytes(), count);
                } else {
                    //c.writeSameEntryBatch(sb.toString().getBytes(), count);
                }
                //c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                c.closeHandle();
            } catch (Exception e) {
                LOG.error("Exception occurred", e);
            } 
        } else if (target.equals("fs") || target.equals("hdfs")) {
            try {
                int numFiles = Integer.valueOf(cmd.getOptionValue("numfiles", "1"));
                
                int numThreads = Math.min(numFiles, 1000);
                byte[] data = sb.toString().getBytes();
                long runid = System.currentTimeMillis();
                List<Callable<Long>> clients = new ArrayList<Callable<Long>>();
                if (target.equals("hdfs")) { 
                    FileSystem fs = FileSystem.get(new Configuration());
                    LOG.info("Default replication for HDFS: {}", fs.getDefaultReplication());

                    List<FSDataOutputStream> streams = new ArrayList<FSDataOutputStream>();
                    for (int i = 0; i < numFiles; i++) {
                        String path = cmd.getOptionValue("path", "/foobar");
                        streams.add(fs.create(new Path(path + runid + "_" + i)));
                    }

                    for (int i = 0; i < numThreads; i++) {
                        clients.add(new HDFSClient(streams, data, runfor));
                    }
                } else {
                    List<FileOutputStream> streams = new ArrayList<FileOutputStream>();
                    for (int i = 0; i < numFiles; i++) {
                        String path = cmd.getOptionValue("path", "/foobar " + i);
                        streams.add(new FileOutputStream(path + runid + "_" + i));
                    }

                    for (int i = 0; i < numThreads; i++) {
                        clients.add(new FileClient(streams, data, runfor));
                    }
                }
                
                ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                
                long start = System.currentTimeMillis();

                List<Future<Long>> results = executor.invokeAll(clients,
                                                                10, TimeUnit.MINUTES);
                long end = System.currentTimeMillis();
                long count = 0;
                for (Future<Long> r : results) {
                    if (!r.isDone()) {
                        LOG.warn("Job didn't complete");
                        System.exit(2);
                    }
                    long c = r.get();
                    if (c == 0) {
                        LOG.warn("Task didn't complete");
                    }
                    count += c;
                }
                long time = end-start;
                LOG.info("Finished processing writes (ms): {} TPT: {} op/s",
                         time, count/((double)time/1000));
                executor.shutdown();
            } catch(Exception e) {
                LOG.error("File not found", e);
            }
        } else {
            System.err.println("Unknown option: " + target);
            System.exit(2);
        }
        timeouter.cancel();
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
        long time = (System.currentTimeMillis() - start);
        LOG.info("Finished processing writes (ms): {} TPT: {} op/s", time, times/((double)time/1000));

        LOG.debug("Ended computation");
    }

    void writeSameEntryBatchSync(byte[] data, int times) throws InterruptedException, BKException {
        start = System.currentTimeMillis();
        int count = times;
        LOG.debug("Data: " + new String(data) + ", " + data.length);
        while(count-- > 0) {
            lh.addEntry(data);
        }
        long time = (System.currentTimeMillis() - start);
        LOG.info("Finished processing writes (ms): {} TPT: {} op/s", time, times/((double)time/1000));

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
        long time = (System.currentTimeMillis() - start);
        LOG.info("Finished processing writes (ms): {} TPT: {} op/s", time, times/((double)time/1000));

        Integer mon = Integer.valueOf(0);
        synchronized(mon) {
            lh.asyncReadEntries(1, times - 1, this, mon);
            mon.wait();
        }
        LOG.error("Ended computation");
    }

    class SyncThread implements Runnable {
        final AtomicInteger outstanding;
        final OutputStream fStream; 
        final AtomicBoolean done;

        SyncThread(AtomicInteger outstanding, AtomicBoolean done, OutputStream fStream) {
            this.outstanding = outstanding;
            this.done = done;
            this.fStream = fStream;
        }
            
        public void run() {
            try {
                while (true) {
                    int toSync = outstanding.get();

                    fStream.flush();
                    if (fStream instanceof FSDataOutputStream) {
                        ((FSDataOutputStream)fStream).hflush();
                    } else if (fStream instanceof FileOutputStream) {
                        ((FileOutputStream)fStream).getChannel().force(false);
                    }
                    outstanding.addAndGet(-1 * toSync);

                    if (outstanding.get() == 0
                        && done.get()) {
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exceptin in sync thread", e);
            }
        }
    }

    static class HDFSClient implements Callable<Long> {
        final List<FSDataOutputStream> streams;
        final byte[] data;
        final long time;
        final Random r;

        HDFSClient(List<FSDataOutputStream> streams, byte[] data, long time) {
            this.streams = streams;
            this.data = data;
            this.time = time;
            this.r = new Random(System.identityHashCode(this));
        }
        
        public Long call() {
            try {
                long count = 0;
                long start = System.currentTimeMillis();
                long stopat = start + time;
                while(System.currentTimeMillis() < stopat) {
                    FSDataOutputStream stream = streams.get(r.nextInt(streams.size()));
                    synchronized(stream) {
                        stream.write(data);
                        stream.flush();
                        stream.hflush();
                    }
                    count++;
                }

                long endtime = System.currentTimeMillis();
                long time = (System.currentTimeMillis() - start);
                LOG.info("Worker finished processing writes (ms): {} TPT: {} op/s",
                         time, count/((double)time/1000));
                return count;
            } catch(Exception e) {
                return 0L;
            }
        }
    }

    static class FileClient implements Callable<Long> {
        final List<FileOutputStream> streams;
        final byte[] data;
        final long time;
        final Random r;

        FileClient(List<FileOutputStream> streams, byte[] data, long time) {
            this.streams = streams;
            this.data = data;
            this.time = time;
            this.r = new Random(System.identityHashCode(this));
        }
        
        public Long call() {
            try {
                long count = 0;
                long start = System.currentTimeMillis();

                long stopat = start + time;
                while(System.currentTimeMillis() < stopat) {
                    FileOutputStream stream = streams.get(r.nextInt(streams.size()));
                    synchronized(stream) {
                        stream.write(data);
                        stream.flush();
                        stream.getChannel().force(false);
                    }
                    count++;
                }

                long endtime = System.currentTimeMillis();
                long time = (System.currentTimeMillis() - start);
                LOG.info("Worker finished processing writes (ms): {} TPT: {} op/s", time, count/((double)time/1000));
                return count;
            } catch(Exception e) {
                return 0L;
            }
        }
    }

    void writeSameEntryBatchFS(byte[] data, int times, boolean sync) {
        int count = times;
        LOG.debug("Data: " + data.length + ", " + times);

        AtomicInteger outstanding = new AtomicInteger(0);
        AtomicBoolean done = new AtomicBoolean(false);
        Thread t = new Thread(new SyncThread(outstanding, done, fStream));
        if (!sync) {
            t.start();
        }
        try {
            start = System.currentTimeMillis();
            while(count-- > 0) {
                fStream.write(data);
                if (sync) {
                    fStream.flush();
                    if (fStream instanceof FSDataOutputStream) {
                        ((FSDataOutputStream)fStream).hflush();
                    } else if (fStream instanceof FileOutputStream) {
                        ((FileOutputStream)fStream).getChannel().force(false);
                    }
                } else {
                    //fStreamLocal.write(data);
                    outstanding.incrementAndGet();
                }
            }

            if (!sync) {
                done.set(true);
                t.join();
                if (outstanding.get() != 0) {
                    throw new IOException("Not all entries synced");
                }
            }
            fStream.close();
            long time = (System.currentTimeMillis() - start);
            LOG.info("Finished processing writes (ms): {} TPT: {} op/s", time, times/((double)time/1000));
        } catch(Exception e) {
            LOG.error("Exception occurred", e);
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
