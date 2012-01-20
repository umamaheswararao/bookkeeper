package org.apache.bookkeeper.benchmark;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event;

import java.util.Enumeration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.concurrent.CountDownLatch;

public class ReaderBenchmark {
    public static void readLedger(String zkservers, long ledgerId) {
        System.out.println("Reading ledger " + ledgerId);
        BookKeeper bk = null;
        long time = 0;
        long entriesRead = 0;
        long lastRead = 0;
        int nochange = 0;

        LedgerHandle lh = null;
        try {
            bk = new BookKeeper(zkservers);
            while (true) {
                lh = bk.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32, new byte[] {'a', 'b'});
                long lastConfirmed = lh.getLastAddConfirmed();
                if (lastConfirmed == lastRead) {
                    nochange++;
                    if (nochange == 10) {
                        break;
                    } else {
                        Thread.sleep(1000);
                        continue;
                    }
                } else {
                    nochange = 0;
                }
                long starttime = System.nanoTime();
                
                Enumeration<LedgerEntry> entries = lh.readEntries(lastRead+1, lastConfirmed);
                lastRead = lastConfirmed;
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    entriesRead++;
                }
                long endtime = System.nanoTime();
                time += endtime - starttime;
                
                lh.close();
                lh = null;
                Thread.sleep(1000);
            }
        } catch (InterruptedException ie) { 
            // ignore
        } catch (Exception e ) {
            System.out.println("Exception in reader " + e.toString());
            e.printStackTrace();
        } finally {
            System.out.println("Read " + entriesRead + " in " + time/1000/1000 + " ms");
            
            try {
                if (lh != null) {
                    lh.close();
                }
                if (bk != null) {
                    bk.close();
                }
            } catch (Exception e) {
                System.out.println("Exception closing stuff");
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final String servers = args[0];
        final long ledgerId = args.length == 2 ? Long.valueOf(args[1]) : 0;

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final String nodepath = String.format("/ledgers/L%010d", ledgerId);
        System.out.println("Node to watch " + nodepath);
        final ZooKeeper zk = new ZooKeeper(servers, 3000, new Watcher() {
                public void process(WatchedEvent event) {
                    System.out.println("W1 " + event);
                    if (event.getState() == Event.KeeperState.SyncConnected 
                            && event.getType() == Event.EventType.None) {
                        connectedLatch.countDown();
                    }
                }
            });
        zk.register(new Watcher() {
                public void process(WatchedEvent event) {
                    System.out.println("W2 " + event);
                    try {
                        if (event.getState() == Event.KeeperState.SyncConnected 
                            && event.getType() == Event.EventType.None) {
                            connectedLatch.countDown();
                        } else if (event.getType() == Event.EventType.NodeCreated
                                   && event.getPath().equals(nodepath)) {
                            readLedger(servers, ledgerId);
                            shutdownLatch.countDown();
                        } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                            List<String> children = zk.getChildren("/ledgers", true);
                            final Pattern p = Pattern.compile("L([0-9]+)$");
                            List<String> ledgers = new ArrayList<String>();
                            for (String child : children) {
                                if (p.matcher(child).find()) {
                                    ledgers.add(child);
                                }
                            }
                            Collections.sort(ledgers, 
                                             new Comparator<String>() {
                                        public int compare(String o1, String o2) {
                                            try {
                                                Matcher m1 = p.matcher(o1);
                                                Matcher m2 = p.matcher(o2);
                                                if (m1.find() && m2.find()) {
                                                    return Integer.valueOf(m1.group(1)) 
                                                        - Integer.valueOf(m2.group(1));
                                                } else {
                                                    return o1.compareTo(o2);
                                                }
                                            } catch (Throwable t) {
                                                return o1.compareTo(o2);
                                            }
                                        }
                                             });
                            String last = ledgers.get(ledgers.size() - 1);
                            final Matcher m = p.matcher(last);
                            if (m.find()) {
                                Thread t = new Thread() {
                                        public void run() {
                                            readLedger(servers, Long.valueOf(m.group(1)));
                                        }
                                    };
                                t.start();
                            } else {
                                System.out.println("Cant file ledger id in " + last);
                            }
                        } else {
                            System.out.println("Unknown event " + event);
                        }
                    } catch (Exception e) {
                        System.out.println("Excepiton in watcher");
                        e.printStackTrace();
                    }
                }
            });
        connectedLatch.await();
        if (ledgerId != 0) {
            if (zk.exists(nodepath, true) != null) {
                readLedger(servers, ledgerId);
                shutdownLatch.countDown();
            }
        } else {
            zk.getChildren("/ledgers", true);
        }
        shutdownLatch.await();
        zk.close();
    }

        
}