package com.cottagecoders.postgreswalreader;

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
      replConnection.getReplicationAPI().createReplicationSlot().logical().withSlotName(cmd.getOptionValue("slotname")).withOutputPlugin(
          cmd.getOptionValue("decoder")).make();

    } catch (SQLException ex) {
      LOG.warn(ex.getMessage());

    }

    try {
      LogSequenceNumber lsn = LogSequenceNumber.valueOf("0/0");

      stream = replConnection.getReplicationAPI()
          .replicationStream()
          .logical()
          .withSlotName(cmd.getOptionValue("slotname"))
          .withStartPosition(lsn)
          .withSlotOption("include-xids", true)
          .withSlotOption("skip-empty-xacts", true)
          .withStatusInterval(20, TimeUnit.SECONDS)
          .start();

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
            System.out.println(e.getKey() + " " + e.getValue());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          e.printStackTrace();
        }
      }
    });

    Options options = new Options();
    options.addOption(new Option("D", "debug", false, "Turn on debug."));
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
    pgwr.run();
  }

  @java.lang.SuppressWarnings("squid:S2189, squid:S2182")
  void run() {

    int lim = 1;
    while (lim < 100000000) {
      ++lim;
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

      String[] parts = new String(source, offset, length).split(" ");

      if (parts[0].equalsIgnoreCase("table")) {
        if (tableCounts.containsKey(parts[1])) {
          tableCounts.put(parts[1], tableCounts.get(parts[1]) + 1);
        } else {
          tableCounts.put(parts[1], 1);
        }
      }

      stream.setAppliedLSN(stream.getLastReceiveLSN());
      stream.setFlushedLSN(stream.getLastReceiveLSN());
    }
  }

}
