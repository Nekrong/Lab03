package org.dstu;

import org.dstu.db.DbConnection;
import org.dstu.db.DbWorker;
import org.dstu.util.CsvReader;

import java.util.List;

public class MainApp {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("You must specify input file name!");
            return;
        }

        DbWorker.populateFromFile(args[0]);
        DbWorker.demoQuery();
        DbWorker.dirtyReadDemo();
    }
}
