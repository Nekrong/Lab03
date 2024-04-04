package org.dstu.db;

import org.dstu.util.CsvReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DbWorker {
    public static void populateFromFile(String fileName) {
        List<String[]> strings = CsvReader.readCsvFile(fileName, ";");
        Connection conn = DbConnection.getConnection();
        try {
            Statement cleaner = conn.createStatement();
            System.out.println(cleaner.executeUpdate("DELETE FROM chair"));
            System.out.println(cleaner.executeUpdate("DELETE FROM tab1e"));
            PreparedStatement chairSt = conn.prepareStatement(
                    "INSERT INTO chair (material, country, manufacturer, color, upholstery) " +
                            "VALUES (?, ?, ?, ?, ?)");
            PreparedStatement tab1eSt = conn.prepareStatement(
                    "INSERT INTO tab1e (material, country, manufacturer, form, type, legs) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");

            for (String[] line: strings) {
                if (line[0].equals("0")) {
                    chairSt.setString(1, line[1]);
                    chairSt.setString(2, line[2]);
                    chairSt.setString(3, line[3]);
                    chairSt.setString(4, line[4]);
                    chairSt.setString(5, line[5]);
                    chairSt.addBatch();
                } else {
                    tab1eSt.setString(1, line[1]);
                    tab1eSt.setString(2, line[2]);
                    tab1eSt.setString(3, line[3]);
                    tab1eSt.setString(4, line[4]);
                    tab1eSt.setString(5, line[5]);
                    tab1eSt.setInt(6, Integer.parseInt(line[6]));
                    tab1eSt.addBatch();
                }
            }
            int[] chRes = chairSt.executeBatch();
            int[] tabRes = tab1eSt.executeBatch();
            for (int num: chRes) {
                System.out.println(num);
            }

            for (int num: tabRes) {
                System.out.println(num);
            }
            cleaner.close();
            chairSt.close();
            tab1eSt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void demoQuery() {
        Connection conn = DbConnection.getConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM tab1e WHERE legs > 2");
            while (rs.next()) {
                System.out.print(rs.getString("material"));
                System.out.print(" ");
                System.out.print(rs.getString("country"));
                System.out.print(" ");
                System.out.println(rs.getString("type"));
            }
            rs.close();
            st.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void dirtyReadDemo() {
        Runnable first = () -> {
            Connection conn1 = DbConnection.getNewConnection();
            if (conn1 != null) {
                try {
                    conn1.setAutoCommit(false);
                    conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement upd = conn1.createStatement();
                    upd.executeUpdate("UPDATE chair SET color='Зеленый' WHERE color='Желтый'");
                    Thread.sleep(2000);
                    conn1.rollback();
                    upd.close();
                    Statement st = conn1.createStatement();
                    System.out.println("In the first thread:");
                    ResultSet rs = st.executeQuery("SELECT * FROM chair");
                    while (rs.next()) {
                        System.out.println(rs.getString("color"));
                    }
                    st.close();
                    rs.close();
                    conn1.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };

        Runnable second = () -> {
            Connection conn2 = DbConnection.getNewConnection();
            if (conn2 != null) {
                try {
                    Thread.sleep(500);
                    conn2.setAutoCommit(false);
                    conn2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement st = conn2.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM chair");
                    while (rs.next()) {
                        System.out.println(rs.getString("color"));
                    }
                    rs.close();
                    st.close();
                    conn2.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };
        Thread th1 = new Thread(first);
        Thread th2 = new Thread(second);
        th1.start();
        th2.start();
    }
}
