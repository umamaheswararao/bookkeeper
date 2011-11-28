package org.apache.bookkeeper.benchmark;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

public class BenchBookie {
    static class LatencyCallback implements WriteCallback {
        boolean complete;
        @Override
        synchronized public void writeComplete(int rc, long ledgerId, long entryId,
                InetSocketAddress addr, Object ctx) {
            if (rc != 0) {
                System.err.println("Got error " + rc);
            }
            complete = true;
            notifyAll();
        }
        synchronized public void resetComplete() {
            complete = false;
        }
        synchronized public void waitForComplete() throws InterruptedException {
            while(!complete) {
                wait();
            }
        }
    }
    
    static class ThroughputCallback implements WriteCallback {
        int count;
        int waitingCount = Integer.MAX_VALUE;
        synchronized public void writeComplete(int rc, long ledgerId, long entryId,
                InetSocketAddress addr, Object ctx) {
            if (rc != 0) {
                System.err.println("Got error " + rc);
            }
            count++;
            if (count >= waitingCount) {
                notifyAll();
            }
        }
        synchronized public void waitFor(int count) throws InterruptedException {
            while(this.count < count) {
                waitingCount = count;
                wait(1000);
            }
            waitingCount = Integer.MAX_VALUE;
        }
    }
    /**
     * @param args
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws InterruptedException {
        if (args.length != 2) {
            System.err.println("USAGE: " + BenchBookie.class.getName() + " address port");
            System.exit(2);
        }
        String addr = args[0];
        int port = Integer.parseInt(args[1]);
        ClientSocketChannelFactory channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                .newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);
       
        BookieClient bc = new BookieClient(channelFactory, executor);
        LatencyCallback lc = new LatencyCallback();
        
        ThroughputCallback tc = new ThroughputCallback();
        int warmUpCount = 999;
        for(long entry = 0; entry < warmUpCount; entry++) {
            ChannelBuffer toSend = ChannelBuffers.buffer(128);
            toSend.resetReaderIndex();
            toSend.resetWriterIndex();
            toSend.writeLong(1);
            toSend.writeLong(entry);
            toSend.writerIndex(toSend.capacity());
            bc.addEntry(new InetSocketAddress(addr, port), 1, new byte[20], entry, toSend, tc, null);
        }
        System.err.println("Waiting for warmup");
        tc.waitFor(warmUpCount);
        
        System.err.println("Benchmarking latency");
        int entryCount = 5000;
        long startTime = System.nanoTime();
        for(long entry = 0; entry < entryCount; entry++) {
            ChannelBuffer toSend = ChannelBuffers.buffer(128);
            toSend.resetReaderIndex();
            toSend.resetWriterIndex();
            toSend.writeLong(2);
            toSend.writeLong(entry);
            toSend.writerIndex(toSend.capacity());
            lc.resetComplete();
            bc.addEntry(new InetSocketAddress(addr, port), 2, new byte[20], entry, toSend, lc, null);
            lc.waitForComplete();
        }
        long endTime = System.nanoTime();
        System.out.println("Latency: " + (((double)(endTime-startTime))/((double)entryCount))/1000000.0);
        
        entryCount = 50000;
        System.err.println("Benchmarking throughput");
        startTime = System.currentTimeMillis();
        tc = new ThroughputCallback();
        for(long entry = 0; entry < entryCount; entry++) {
            ChannelBuffer toSend = ChannelBuffers.buffer(128);
            toSend.resetReaderIndex();
            toSend.resetWriterIndex();
            toSend.writeLong(3);
            toSend.writeLong(entry);
            toSend.writerIndex(toSend.capacity());
            bc.addEntry(new InetSocketAddress(addr, port), 3, new byte[20], entry, toSend, tc, null);
            // throttel at 5000
            tc.waitFor((int)entry-5000);
        }
        tc.waitFor(entryCount);
        endTime = System.currentTimeMillis();
        System.out.println("Throughput: " + ((long)entryCount)*1000/(endTime-startTime));
        System.exit(0);
    }

}
