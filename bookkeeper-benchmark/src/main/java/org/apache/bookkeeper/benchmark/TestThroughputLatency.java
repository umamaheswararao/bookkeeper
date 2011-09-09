package org.apache.bookkeeper.benchmark;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class TestThroughputLatency implements AddCallback, Runnable {
    static Logger LOG = Logger.getLogger(TestThroughputLatency.class);

    BookKeeper bk;
    LedgerHandle lh;
    AtomicLong counter;
    AtomicLong completions = new AtomicLong(0);
    Semaphore sem;
    long length;
    int paceInNanos;
    int throttle;
    
    class Context {
        long localStartTime;
        long globalStartTime;
        long id;
        
        Context(long id, long time){
            this.id = id;
            this.localStartTime = this.globalStartTime = time;
        }
    }
    
    public TestThroughputLatency(int paceInNanos, String length, String ensemble, String qSize, String throttle, String servers) 
    throws KeeperException, 
        IOException, 
        InterruptedException {
        //this.sem = new Semaphore(Integer.parseInt(throttle));
        this.paceInNanos = paceInNanos;
        System.setProperty("throttle", throttle);
        this.throttle = Integer.parseInt(throttle);
        bk = new BookKeeper(servers);
        this.length = Long.parseLong(length);
        this.counter = new AtomicLong(0);
        try{
            //System.setProperty("throttle", throttle.toString());
            lh = bk.createLedger(Integer.parseInt(ensemble), Integer.parseInt(qSize), BookKeeper.DigestType.CRC32, new byte[] {'a', 'b'});
        } catch (BKException e) {
            e.printStackTrace();
        } 
    }
    
    public void close() 
    throws InterruptedException {
        lh.close();
        bk.halt();
    }
    
    long previous = 0;
    byte bytes[];
    
    void setEntryData(String data) {
        bytes = data.getBytes();
    }
    
    public void run() {
        LOG.info("Running...");
        long start = previous = System.currentTimeMillis();
        long millis = paceInNanos/1000000;
        int nanos = paceInNanos%1000000;
        long lastNanoTime = System.nanoTime();
        byte messageCount = 0;
        while(!Thread.currentThread().isInterrupted()) {
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
            int limit = (int) (throttle - counter.get());
            if (toSend > limit) {
                toSend = limit;
            }
            for(int i = 0; i < toSend; i++) {
                lh.asyncAddEntry(bytes, this, new Context(counter.getAndIncrement(), nanoTime));
            }
            lastNanoTime = nanoTime;
        }
        
        try {
            synchronized (this) {
                while(this.counter.get() == throttle)
                    wait();
            }
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
        LOG.debug("Finished processing in ms: " + (System.currentTimeMillis() - start));
        LOG.debug("Ended computation");
        System.out.flush();
    }
    
    long threshold = 20000;
    long runningAverage = 0;
    long runningAverageCounter = 1;
    long totalTime = 0;
    volatile double avgLatency = 0;
    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        Context context = (Context) ctx;
        
        completions.incrementAndGet();
        if((entryId % 500) == 0){ 
            long newTime = System.nanoTime() - context.localStartTime;
            totalTime += newTime; 
            ++runningAverageCounter;
            //runningAverage = (newTime + ((runningAverageCounter) * runningAverage))/(++runningAverageCounter);
            avgLatency = ((double)totalTime /(double)runningAverageCounter)/1000000.0;
        }
            
        if(threshold - (entryId % threshold) == 1){
            long diff = System.currentTimeMillis() - previous;
            long toOutput = entryId + 1;
            //System.out.println("SAMPLE\t" + toOutput + "\t" + diff);
            previous = System.currentTimeMillis();
            if(runningAverageCounter > 0){
                avgLatency = ((double)totalTime /(double)runningAverageCounter)/1000000.0;
            }
            //runningAverage = 0;
            // totalTime = 0;
            // runningAverageCounter = 0;
            System.out.println("SAMPLE\t" + toOutput + "\t" + diff + "\t" + avgLatency);
        }
        
        //sem.release();
        synchronized(this) {
            // we the counter was at throttle we need to notify
            if(counter.decrementAndGet() == throttle-1)
                notify();
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
        if (args.length < 6) {
            System.err.println("USAGE: " + TestThroughputLatency.class.getName() + " running_time(secs) sizeof_entry ensemble_size quorum_size throttle throughput(ops/sec) zk_server\n");
            System.exit(2);
        }
        StringBuffer servers_sb = new StringBuffer();
        for (int i = 6; i < args.length; i++){
            servers_sb.append(args[i] + " ");
        }
    
        long runningTime = Long.parseLong(args[0]);
        String servers = servers_sb.toString().trim().replace(' ', ',');
        LOG.warn("(Parameters received) running time: " + args[0] + 
                ", Length: " + args[1] + ", ensemble size: " + args[2] + 
                ", quorum size" + args[3] + 
                ", throttle: " + args[4] + 
                ", throughput(ops/sec): " + args[5] +
                ", zk servers: " + servers);
        final int opsPerSec = Integer.parseInt(args[5]);
        int paceInNanos = 0;
        if (opsPerSec != 0) {
            paceInNanos = 1000000000/opsPerSec;
        }
        TestThroughputLatency ttl = new TestThroughputLatency(paceInNanos, args[1], args[2], args[3], args[4], servers);
        
        int length = Integer.parseInt(args[1]);
        StringBuffer sb = new StringBuffer();
        while(length-- > 0){
            sb.append('a');
        }
        long totalTime = runningTime*1000;
        ttl.setEntryData(sb.toString());
        final Thread thread = new Thread(ttl);
        thread.start();
        Thread.sleep(totalTime);
        thread.interrupt();
        final long completionCount = ttl.completions.get();
        double tp = (double)completionCount/(double)runningTime;
        System.out.println(completionCount + " completions in " + totalTime + " seconds: " + tp + " ops/sec");
        System.out.println("Average latency: " + ttl.avgLatency);
        Runtime.getRuntime().halt(0);
    }
}
