
package org.apache.bookkeeper.bookie;

import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.conf.ServerConfiguration;
import java.net.URL;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class BookieLatTest {
    public static void main(String[] args) throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.loadConf(new URL("file:///grid/2/dev/ivank/bkbench/bookkeeper/bookkeeper-server/conf/bk_server.conf"));
        conf.setZkServers(null);
        Bookie b = new Bookie(conf);
        b.start();

        int numreq = Integer.valueOf(args[0]);
        byte[] key = "foobar".getBytes();
        byte[] data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".getBytes();
        final CountDownLatch latch = new CountDownLatch(100);

        WriteCallback warmupcb = new WriteCallback() {
                public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {
                    latch.countDown();
                }
            };

        WriteCallback cb = new WriteCallback() {
                public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {
                    long start = (Long)ctx;
                    long end = System.currentTimeMillis();
                    System.out.println("entryID " + entryId + " took " + (end-start) + "ms");                    
                }
            };

        for (int i = 0; i < 100; i++) {
            ByteBuffer entry = ByteBuffer.allocate(128);
            entry.putLong(1);
            entry.putLong(i+1);
            entry.put(data);
            entry.flip();

            b.addEntry(entry, warmupcb, System.currentTimeMillis(), key);
        }
        latch.await();

        for (int i = 0; i < numreq; i++) {
            ByteBuffer entry = ByteBuffer.allocate(128);
            entry.putLong(1);
            entry.putLong(100+i);
            entry.put(data);
            entry.flip();

            b.addEntry(entry, cb, System.currentTimeMillis(), key);
        }

    }
}