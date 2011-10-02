package org.apache.bookkeeper.benchmark;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.util.MainUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class TestThroughputLatency implements AddCallback, Runnable {
    static Logger LOG = Logger.getLogger(TestThroughputLatency.class);

    BookKeeper bk;
    LedgerHandle lh[];
    int counter;
    int completions = 0;
    Semaphore sem;
    int paceInNanos;
    int throttle;
    int numberOfLedgers = 1;
    
    class Context {
        long localStartTime;
        long globalStartTime;
        long id;
        
        Context(long id, long time){
            this.id = id;
            this.localStartTime = this.globalStartTime = time;
        }
    }
    
    public TestThroughputLatency(int paceInNanos, int ensemble, int qSize, int throttle, int numberOfLedgers, String servers) 
    throws KeeperException, 
        IOException, 
        InterruptedException {
        //this.sem = new Semaphore(Integer.parseInt(throttle));
        this.paceInNanos = paceInNanos;
        System.setProperty("throttle", Integer.toString(throttle));
        this.throttle = throttle;
        bk = new BookKeeper(servers);
        this.counter = 0;
        this.numberOfLedgers = numberOfLedgers;
        try{
            //System.setProperty("throttle", throttle.toString());
            lh = new LedgerHandle[this.numberOfLedgers];
            for(int i = 0; i < this.numberOfLedgers; i++) {
                lh[i] = bk.createLedger(ensemble, qSize, BookKeeper.DigestType.CRC32, new byte[] {'a', 'b'});
            }
        } catch (BKException e) {
            e.printStackTrace();
        } 
    }
    
    Random rand = new Random();
    public void close() throws InterruptedException {
        for(int i = 0; i < numberOfLedgers; i++) {
            lh[i].close();
        }
        bk.halt();
    }
    
    long previous = 0;
    byte bytes[];
    
    void setEntryData(byte data[]) {
        bytes = data;
    }
    
    int lastLedger = 0;
    private int getRandomLedger() {
        // return rand.nextInt(numberOfLedgers);
        lastLedger = (lastLedger+1)%numberOfLedgers;
        return lastLedger;
    }
    
    long latencies[] = new long[1000000];
    int latencyIndex = -1;
    int sendLimit = Integer.MAX_VALUE;
    public void setSendLimit(int sendLimit) {
        this.sendLimit = sendLimit;
        latencies = new long[sendLimit];
    }
    
    public void run() {
        LOG.info("Running...");
        long start = previous = System.currentTimeMillis();
        long millis = paceInNanos/1000000;
        int nanos = paceInNanos%1000000;
        long lastNanoTime = System.nanoTime();
        byte messageCount = 0;
        int sent = 0;
        while(!Thread.currentThread().isInterrupted() && sent < sendLimit) {
            if (paceInNanos > 0) {
                try {
                    Thread.sleep(millis, nanos);
                } catch (InterruptedException e) {
                    break;
                }
            }
            //sem.acquire();
            long nanoTime = System.nanoTime();
            int toSend = throttle;
            if (paceInNanos > 0) {
                toSend = (int) ((nanoTime-lastNanoTime)/paceInNanos);
                if (toSend > 100 && (++messageCount&0xff) < 5) {
                    LOG.error("We are sending " + toSend + " ops in this interval");
                }
            }
            synchronized(this) {
                int limit = (int) (throttle - counter);
                if (toSend > limit) {
                    toSend = limit;
                }
                counter += toSend;
            }
            for(int i = 0; i < toSend; i++) {
                final int index = getRandomLedger();
                LedgerHandle h = lh[index];
                if (h == null) {
                    LOG.error("Handle " + index + " is null!");
                } else {
                    lh[index].asyncAddEntry(bytes, this, new Context(sent, nanoTime));
                }
                sent++;
            }
            lastNanoTime = nanoTime;
        }
        
        try {
            synchronized (this) {
                while(this.counter > 0)
                    wait();
            }
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
        final long duration = System.currentTimeMillis() - start;
        throughput = sent*1000/duration;
        LOG.info("Finished processing in ms: " + duration + " tp = " + throughput);
        System.out.flush();
    }
    
    long throughput = -1;
    public long getThroughput() {
        return throughput;
    }
    
    long threshold = 20000;
    long runningAverageCounter = 0;
    long totalTime = 0;
    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        Context context = (Context) ctx;
        
        // we need to use the id passed in the context in the case of
        // multiple ledgers, and it works even with one ledger
        entryId = context.id;
        long newTime = System.nanoTime() - context.localStartTime;
        long tt = -1;
        long rac = -1;
        int c = -1;
        synchronized(this) {
            runningAverageCounter++;
            latencies[(int)entryId] = newTime;
            if (latencyIndex >= entryId) {
                LOG.error("On entry " + entryId + " ledgerIndex = " + latencyIndex);
            }
            latencyIndex = (int)entryId;
            totalTime += newTime;
            completions++;
            tt = totalTime;
            rac = runningAverageCounter;
            counter--;
            c = counter;
            notify();
        }
        
        if((entryId % threshold) == (threshold - 1)){
            final long now = System.currentTimeMillis();
            long diff = now - previous;
            long toOutput = entryId + 1;
            double avgLatency = -1;
            //System.out.println("SAMPLE\t" + toOutput + "\t" + diff);
            previous = now;
            if(rac > 0){
                avgLatency = ((double)tt/(double)rac)/1000000.0;
            }
            //runningAverage = 0;
            // totalTime = 0;
            // runningAverageCounter = 0;
            System.out.println("SAMPLE\t" + toOutput + "\t" + diff + "\t" + avgLatency + "\t" + c);
        }
    }
    
    /**
     * Argument 0 is the number of entries to add
     * Argument 1 is the length of entries
     * Argument 2 is the ensemble size
     * Argument 3 is the quorum size
     * Argument 4 is the throttle threshold
     * Argument 5 is the address of the ZooKeeper server
     * 
     * @param args
     * @throws KeeperException
     * @throws IOException
     * @throws InterruptedException
     */
    
    public static void main(String[] args) 
    throws KeeperException, IOException, InterruptedException {
        if (args.length < 7) {
            System.err.println("USAGE: " + TestThroughputLatency.class.getName() + " running_time(secs) sizeof_entry ensemble_size quorum_size throttle throughput(ops/sec) number_of_ledgers zk_server\n");
            System.exit(2);
        }
        StringBuffer servers_sb = new StringBuffer();
        for (int i = 7; i < args.length; i++){
            servers_sb.append(args[i] + " ");
        }
    
        MainUtil.outputInitInfo();
        long runningTime = Long.parseLong(args[0]);
        String servers = servers_sb.toString().trim().replace(' ', ',');
        LOG.warn("(Parameters received) running time: " + args[0] + 
                ", Length: " + args[1] + ", ensemble size: " + args[2] + 
                ", quorum size" + args[3] + 
                ", throttle: " + args[4] + 
                ", throughput(ops/sec): " + args[5] +
                ", number of ledgers: " + args[6] +
                ", zk servers: " + servers);
        final int opsPerSec = Integer.parseInt(args[5]);
        int paceInNanos = 0;
        if (opsPerSec != 0) {
            paceInNanos = 1000000000/opsPerSec;
        }
        int length = Integer.parseInt(args[1]);
        StringBuffer sb = new StringBuffer();
        while(length-- > 0){
            sb.append('a');
        }
        byte data[] = sb.toString().getBytes();
        long totalTime = runningTime*1000;
        
        // Do a warmup run
        int ledgers = Integer.parseInt(args[6]);
        int ensemble = Integer.parseInt(args[2]);
        int qSize = Integer.parseInt(args[3]);
        int throttle = Integer.parseInt(args[4]);
        Thread thread;
        /*
        long lastWarmUpTP = -1;
        long throughput;
        LOG.info("Starting warmup");
        while(lastWarmUpTP < (throughput = warmUp(servers, paceInNanos, data, ledgers, ensemble, qSize, throttle))) {
            LOG.info("Warmup tp: " + throughput);
            lastWarmUpTP = throughput;
            // we will just run once, so lets break
            break;
        }
        */
        
        LOG.info("Warmup phase finished");
        
        // Now do the benchmark
        TestThroughputLatency ttl = new TestThroughputLatency(paceInNanos, ensemble, qSize, throttle, ledgers, servers);
        ttl.setEntryData(data);
        thread = new Thread(ttl);
        thread.start();
        Thread.sleep(totalTime);
        long rac = -1;
        long tt = -1;
        long cc = -1;
        synchronized(ttl) {
            rac = ttl.runningAverageCounter;
            tt = ttl.totalTime;
            cc = ttl.completions;
        }
        thread.interrupt();
        double tp = (double)cc/(double)runningTime;
        System.out.println(cc + " completions in " + totalTime + " seconds: " + tp + " ops/sec");
        System.out.println("Average latency: " + ((double)tt /(double)rac)/1000000.0);
        ArrayList<Long> latency = new ArrayList<Long>();
        for(int i = 0; i < ttl.latencyIndex; i++) {
            latency.add(ttl.latencies[i]);
        }
        Collections.sort(latency);
        System.out.println("99th percentile latency: " + percentile(latency, 99));
        System.out.println("95th percentile latency: " + percentile(latency, 95));
        OutputStream fos = new BufferedOutputStream(new FileOutputStream("latencyDump.dat"));
        
        for(Long l: latency) {
            fos.write((Long.toString(l)+"\n").getBytes());
        }
        fos.flush();
        Runtime.getRuntime().halt(0);
    }

    private static double percentile(ArrayList<Long> latency, int percentile) {
        int size = latency.size();
        int sampleSize = (size * percentile) / 100;
        int skip = size - sampleSize;
        long total = 0;
        for(int i = skip; i < size; i++) {
            total += latency.get(i);
        }
        return (double)total/(double)sampleSize;
    }

    private static long warmUp(String servers, int paceInNanos, byte[] data,
            int ledgers, int ensemble, int qSize, int throttle)
            throws KeeperException, IOException, InterruptedException {
        TestThroughputLatency ttl = new TestThroughputLatency(paceInNanos, ensemble, qSize, throttle, ledgers, servers);
        int limit = ledgers*3;
        if (limit < 50000) {
            limit = 50000;
        }
        ttl.setSendLimit(limit);
        ttl.setEntryData(data);
        Thread thread = new Thread(ttl);
        thread.start();
        thread.join();
        return ttl.getThroughput();
    }
}
