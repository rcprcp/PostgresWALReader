package com.cottagecoders.postgreswalreader;

import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PostgresWALReader {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresWALReader.class);
  private static final String EXCEPTION = "Exception {}: ";
  private static final Map<String, Integer> tableCounts = new HashMap<>();
  Connection conn;
  PGConnection replConnection;
  PGReplicationStream stream;

  PostgresWALReader(CommandLine cmd) {

    Properties props = new Properties();
    PGProperty.USER.set(props, cmd.getOptionValue("username"));
    PGProperty.PASSWORD.set(props, cmd.getOptionValue("password"));
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.6");
    PGProperty.REPLICATION.set(props, "database");
    PGProperty.PREFER_QUERY_MODE.set(props, "simple");

    try {
      conn = DriverManager.getConnection(cmd.getOptionValue("jdbcurl"), props);
      replConnection = conn.unwrap(PGConnection.class);
    } catch (SQLException ex) {
      LOG.error(EXCEPTION, ex.getMessage(), ex);
      System.exit(4);
    }

    //configure the replication decoding plugin.
    try {
      replConnection.getReplicationAPI()
          .createReplicationSlot()
          .logical()
          .withSlotName(cmd.getOptionValue("slotname"))
          .withOutputPlugin(cmd.getOptionValue("decoder"))
          .make();

    } catch (SQLException ex) {
      LOG.warn("Exception: {}", ex.getMessage(), ex);

    }

    try {
      LogSequenceNumber lsn = LogSequenceNumber.valueOf("0/0");

      stream = replConnection.getReplicationAPI().replicationStream().logical().withSlotName(cmd.getOptionValue(
          "slotname")).withStartPosition(lsn).withSlotOption("include-xids", true)
          //.withSlotOption("skip-empty-xacts", true)
          .withStatusInterval(20, TimeUnit.SECONDS).start();

    } catch (SQLException ex) {
      LOG.error(EXCEPTION, ex.getMessage(), ex);
      System.exit(3);

    }
  }

  public static void main(String... args) {

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(200);
          System.out.println("Shutting down ...");
          for (Map.Entry<String, Integer> e : tableCounts.entrySet()) {
            System.out.println(e.getKey() + ": " + e.getValue());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          e.printStackTrace();
        }
      }
    });

    Options options = new Options();
    options.addOption(new Option("D", "debug", false, "Enable DEBUG-level log statements."));
    options.addOption(new Option("j", "jdbcurl", true, "JDBC Url"));
    options.addOption(new Option("u", "username", true, "Username"));
    options.addOption(new Option("p", "password", true, "Password"));
    options.addOption(new Option("s", "slotname", true, "Replication slot name"));
    options.addOption(new Option("d", "decoder", true, "Decoder Plugin [wal2json|test-decoding"));

    //    HelpFormatter formatter = new HelpFormatter();
    //    String [] parts = System.getProperty("sun.java.command").split(" ");
    //    formatter.printHelp( parts[0], options );

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException ex) {
      LOG.error(EXCEPTION, ex.getMessage(), ex);
      System.exit(2);
    }

    PostgresWALReader pgwr = new PostgresWALReader(cmd);
    pgwr.run(cmd);
  }

  void run(CommandLine cmd) {

    while (true) {   //NOSONAR S2189 S2182
      ByteBuffer msg = null;
      try {
        msg = stream.readPending();

      } catch (SQLException ex) {
        LOG.info("stream read error {} ", ex.getMessage(), ex);
        System.exit(5);

      }

      try {
        if (msg == null) {
          TimeUnit.MILLISECONDS.sleep(100L);
          continue;
        }

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        System.exit(8);

      }

      int offset = msg.arrayOffset();
      byte[] source = msg.array();
      int length = source.length - offset;

      // print data here.
      LOG.info("{} {}", stream.getLastReceiveLSN().asString(), new String(source, offset, length));
      String theText = new String(source, offset, length);

      if (cmd.getOptionValue("decoder").equalsIgnoreCase("wal2json")) {
        Gson gson = new Gson();
        Wal2JsonRecord w2j = gson.fromJson(theText, Wal2JsonRecord.class);
        System.out.println("hello");
        for(Change c : w2j.getChange()) {
          String schema = c.getSchema();
          String tableName = c.getTable();
          increment(schema, tableName);
        }

      } else if (cmd.getOptionValue("decoder").equalsIgnoreCase("test_decode")) {
        String[] parts = theText.split(" ");
        // TODO: need better error checking here.
        String tableName = parts[0];
        String schema = parts[1];
        increment(schema, tableName);

      }

      stream.setAppliedLSN(stream.getLastReceiveLSN());
      stream.setFlushedLSN(stream.getLastReceiveLSN());
    }
  }

  void increment(String schema, String tableName) {
    String key = schema + ":" + tableName;

    if (tableCounts.containsKey(key)) {
      tableCounts.put(key, tableCounts.get(key) + 1);
    } else {
      tableCounts.put(key, 1);
    }
  }
}
