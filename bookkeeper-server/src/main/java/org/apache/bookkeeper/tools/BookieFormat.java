package org.apache.bookkeeper.tools;

import java.io.File;
import java.io.IOException;

import org.apache.bookkeeper.bookie.LedgerCache;

public class BookieFormat {
    static public void main(String args[]) throws IOException {
        if (args.length < 2) {
            System.err.println("USAGE: " + BookieFormat.class.getName() + " logDir ledgerDir(s) ...");
            System.exit(2);
        }
        for(int i = 1; i < args.length; i++) {
            File f = new File(args[i]);
            LedgerCache.formatLedgerDir(f);
        }
    }
}
