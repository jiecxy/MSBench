/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package cn.ac.ict;


import cn.ac.ict.exception.MSException;
import cn.ac.ict.exception.WorkloadException;
import cn.ac.ict.measurements.Measurements;
import cn.ac.ict.utils.Utils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static cn.ac.ict.constants.Constants.*;
import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * A thread to periodically show the status of the experiment to reassure you that progress is being made.
 */
class StatusThread extends Thread {
  // Counts down each of the clients completing
  private final CountDownLatch completeLatch;

  // Stores the measurements for the run
  private final Measurements measurements;

  // Whether or not to track the JVM stats per run
  private final boolean trackJVMStats;


  private final String label;
  private final boolean standardstatus;

  // The interval for reporting status.
  private long sleeptimeNs;

  // JVM max/mins
  private int maxThreads;
  private int minThreads = Integer.MAX_VALUE;
  private long maxUsedMem;
  private long minUsedMem = Long.MAX_VALUE;
  private double maxLoadAvg;
  private double minLoadAvg = Double.MAX_VALUE;
  private long lastGCCount = 0;
  private long lastGCTime = 0;

  /**
   * Creates a new StatusThread without JVM stat tracking.
   *
   * @param completeLatch         The latch that each client thread will {@link CountDownLatch#countDown()}
   *                              as they complete.
   * @param clients               The clients to collect metrics from.
   * @param label                 The label for the status.
   * @param standardstatus        If true the status is printed to stdout in addition to stderr.
   * @param statusIntervalSeconds The number of seconds between status updates.
   */
  public StatusThread(CountDownLatch completeLatch, List<ClientThread> clients,
                      String label, boolean standardstatus, int statusIntervalSeconds) {
    this(completeLatch, clients, label, standardstatus, statusIntervalSeconds, false);
  }

  /**
   * Creates a new StatusThread.
   *
   * @param completeLatch         The latch that each client thread will {@link CountDownLatch#countDown()}
   *                              as they complete.
   * @param clients               The clients to collect metrics from.
   * @param label                 The label for the status.
   * @param standardstatus        If true the status is printed to stdout in addition to stderr.
   * @param statusIntervalSeconds The number of seconds between status updates.
   * @param trackJVMStats         Whether or not to track JVM stats.
   */
  public StatusThread(CountDownLatch completeLatch, List<ClientThread> clients,
                      String label, boolean standardstatus, int statusIntervalSeconds,
                      boolean trackJVMStats) {
    this.completeLatch = completeLatch;
    this.label = label;
    this.standardstatus = standardstatus;
    sleeptimeNs = TimeUnit.SECONDS.toNanos(statusIntervalSeconds);
    measurements = Measurements.getMeasurements();
    this.trackJVMStats = trackJVMStats;
  }

  /**
   * Run and periodically report status.
   */
  @Override
  public void run() {
    final long startTimeMs = System.currentTimeMillis();
    final long startTimeNanos = System.nanoTime();
    long deadline = startTimeNanos + sleeptimeNs;
    long startIntervalMs = startTimeMs;
    long lastTotalOps = 0;

    boolean alldone;

    do {
      long nowMs = System.currentTimeMillis();

      lastTotalOps = computeStats(startTimeMs, startIntervalMs, nowMs, lastTotalOps);

      if (trackJVMStats) {
        measureJVM();
      }

      alldone = waitForClientsUntil(deadline);

      startIntervalMs = nowMs;
      deadline += sleeptimeNs;
    }
    while (!alldone);

    if (trackJVMStats) {
      measureJVM();
    }
    // Print the final stats.
    computeStats(startTimeMs, startIntervalMs, System.currentTimeMillis(), lastTotalOps);
  }

  /**
   * Computes and prints the stats.
   *
   * @param startTimeMs     The start time of the test.
   * @param startIntervalMs The start time of this interval.
   * @param endIntervalMs   The end time (now) for the interval.
   * @param lastTotalOps    The last total operations count.
   * @return The current operation count.
   */
  private long computeStats(final long startTimeMs, long startIntervalMs, long endIntervalMs,
                            long lastTotalOps) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    long totalops = 0;
    long todoops = 0;

    // Calculate the total number of operations completed.
    for (ClientThread t : clients) {
      totalops += t.getOpsDone();
      todoops += t.getOpsTodo();
    }


    long interval = endIntervalMs - startTimeMs;
    double throughput = 1000.0 * (((double) totalops) / (double) interval);
    double curthroughput = 1000.0 * (((double) (totalops - lastTotalOps)) /
            ((double) (endIntervalMs - startIntervalMs)));
    long estremaining = (long) Math.ceil(todoops / throughput);


    DecimalFormat d = new DecimalFormat("#.##");
    String labelString = this.label + format.format(new Date());

    StringBuilder msg = new StringBuilder(labelString).append(" ").append(interval / 1000).append(" sec: ");
    msg.append(totalops).append(" operations; ");

    if (totalops != 0) {
      msg.append(d.format(curthroughput)).append(" current ops/sec; ");
    }
    if (todoops != 0) {
      msg.append("est completion in ").append(RemainingFormatter.format(estremaining));
    }

    msg.append(Measurements.getMeasurements().getSummary());

    System.err.println(msg);

    if (standardstatus) {
      System.out.println(msg);
    }
    return totalops;
  }

  /**
   * Waits for all of the client to finish or the deadline to expire.
   *
   * @param deadline The current deadline.
   * @return True if all of the clients completed.
   */
  private boolean waitForClientsUntil(long deadline) {
    boolean alldone = false;
    long now = System.nanoTime();

    while (!alldone && now < deadline) {
      try {
        alldone = completeLatch.await(deadline - now, TimeUnit.NANOSECONDS);
      } catch (InterruptedException ie) {
        // If we are interrupted the thread is being asked to shutdown.
        // Return true to indicate that and reset the interrupt state
        // of the thread.
        Thread.currentThread().interrupt();
        alldone = true;
      }
      now = System.nanoTime();
    }

    return alldone;
  }

  /**
   * Executes the JVM measurements.
   */
  private void measureJVM() {
    final int threads = Utils.getActiveThreadCount();
    if (threads < minThreads) {
      minThreads = threads;
    }
    if (threads > maxThreads) {
      maxThreads = threads;
    }
    measurements.measure("THREAD_COUNT", threads);

    // TODO - once measurements allow for other number types, switch to using
    // the raw bytes. Otherwise we can track in MB to avoid negative values 
    // when faced with huge heaps.
    final int usedMem = Utils.getUsedMemoryMegaBytes();
    if (usedMem < minUsedMem) {
      minUsedMem = usedMem;
    }
    if (usedMem > maxUsedMem) {
      maxUsedMem = usedMem;
    }
    measurements.measure("USED_MEM_MB", usedMem);

    // Some JVMs may not implement this feature so if the value is less than
    // zero, just ommit it.
    final double systemLoad = Utils.getSystemLoadAverage();
    if (systemLoad >= 0) {
      // TODO - store the double if measurements allows for them
      measurements.measure("SYS_LOAD_AVG", (int) systemLoad);
      if (systemLoad > maxLoadAvg) {
        maxLoadAvg = systemLoad;
      }
      if (systemLoad < minLoadAvg) {
        minLoadAvg = systemLoad;
      }
    }

    final long gcs = Utils.getGCTotalCollectionCount();
    measurements.measure("GCS", (int) (gcs - lastGCCount));
    final long gcTime = Utils.getGCTotalTime();
    measurements.measure("GCS_TIME", (int) (gcTime - lastGCTime));
    lastGCCount = gcs;
    lastGCTime = gcTime;
  }

  /**
   * @return The maximum threads running during the test.
   */
  public int getMaxThreads() {
    return maxThreads;
  }

  /**
   * @return The minimum threads running during the test.
   */
  public int getMinThreads() {
    return minThreads;
  }

  /**
   * @return The maximum memory used during the test.
   */
  public long getMaxUsedMem() {
    return maxUsedMem;
  }

  /**
   * @return The minimum memory used during the test.
   */
  public long getMinUsedMem() {
    return minUsedMem;
  }

  /**
   * @return The maximum load average during the test.
   */
  public double getMaxLoadAvg() {
    return maxLoadAvg;
  }

  /**
   * @return The minimum load average during the test.
   */
  public double getMinLoadAvg() {
    return minLoadAvg;
  }

  /**
   * @return Whether or not the thread is tracking JVM stats.
   */
  public boolean trackJVMStats() {
    return trackJVMStats;
  }
}

/**
 * Turn seconds remaining into more useful units.
 * i.e. if there are hours or days worth of seconds, use them.
 */
final class RemainingFormatter {
  private RemainingFormatter() {
    // not used
  }

  public static StringBuilder format(long seconds) {
    StringBuilder time = new StringBuilder();
    long days = TimeUnit.SECONDS.toDays(seconds);
    if (days > 0) {
      time.append(days).append(" days ");
      seconds -= TimeUnit.DAYS.toSeconds(days);
    }
    long hours = TimeUnit.SECONDS.toHours(seconds);
    if (hours > 0) {
      time.append(hours).append(" hours ");
      seconds -= TimeUnit.HOURS.toSeconds(hours);
    }
    /* Only include minute granularity if we're < 1 day. */
    if (days < 1) {
      long minutes = TimeUnit.SECONDS.toMinutes(seconds);
      if (minutes > 0) {
        time.append(minutes).append(" minutes ");
        seconds -= TimeUnit.MINUTES.toSeconds(seconds);
      }
    }
    /* Only bother to include seconds if we're < 1 minute */
    if (time.length() == 0) {
      time.append(seconds).append(" seconds ");
    }
    return time;
  }
}

/**
 * A thread for executing transactions or data inserts to the database.
 */
class ClientThread implements Runnable {
  // Counts down each of the clients completing.
  private final CountDownLatch completeLatch;

  private static boolean spinSleep;
  private MS ms;
  private boolean dotransactions;
  private Workload workload;
  private int opcount;
  private double targetOpsPerMs;

  private int opsdone;
  private int threadid;
  private int threadcount;
  private Object workloadstate;
  private Properties props;
  private long targetOpsTickNs;
  private final Measurements measurements;

  /**
   * Constructor.
   *
   * @param ms                   the DB implementation to use
   * @param dotransactions       true to do transactions, false to insert data
   * @param workload             the workload to use
   * @param props                the properties defining the experiment
   * @param opcount              the number of operations (transactions or inserts) to do
   * @param targetperthreadperms target number of operations per thread per ms
   * @param completeLatch        The latch tracking the completion of all clients.
   */
  public ClientThread(MS ms, boolean dotransactions, Workload workload, Properties props, int opcount,
                      double targetperthreadperms, CountDownLatch completeLatch) {
    this.ms = ms;
    this.dotransactions = dotransactions;
    this.workload = workload;
    this.opcount = opcount;
    opsdone = 0;
    if (targetperthreadperms > 0) {
      targetOpsPerMs = targetperthreadperms;
      targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
    }
    this.props = props;
    measurements = Measurements.getMeasurements();
    spinSleep = Boolean.valueOf(this.props.getProperty("spin.sleep", "false"));
    this.completeLatch = completeLatch;
  }

  public int getOpsDone() {
    return opsdone;
  }

  @Override
  public void run() {
    try {
      ms.init();
    } catch (MSException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }

    try {
      workloadstate = workload.initThread(props, threadid, threadcount);
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }

    //TODO 这里放场景类初始化
    //NOTE: Switching to using nanoTime and parkNanos for time management here such that the measurements
    // and the client thread have the same view on time.

    //spread the thread operations out so they don't all hit the DB at the same time
    // GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
    // and the sleep() doesn't make sense for granularities < 1 ms anyway
    if ((targetOpsPerMs > 0) && (targetOpsPerMs <= 1.0)) {
      long randomMinorDelay = Utils.random().nextInt((int) targetOpsTickNs);
      sleepUntil(System.nanoTime() + randomMinorDelay);
    }
    try {
      if (dotransactions) {
        long startTimeNanos = System.nanoTime();

        while (((opcount == 0) || (opsdone < opcount)) && !workload.isStopRequested()) {

          if (!workload.doTransaction(db, workloadstate)) {
            break;
          }

          opsdone++;

          throttleNanos(startTimeNanos);
        }
      } else {
        long startTimeNanos = System.nanoTime();

        while (((opcount == 0) || (opsdone < opcount)) && !workload.isStopRequested()) {

          if (!workload.doInsert(db, workloadstate)) {
            break;
          }

          opsdone++;

          throttleNanos(startTimeNanos);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      measurements.setIntendedStartTimeNs(0);
      db.cleanup();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
    } finally {
      completeLatch.countDown();
    }
  }

  private static void sleepUntil(long deadline) {
    while (System.nanoTime() < deadline) {
      if (!spinSleep) {
        LockSupport.parkNanos(deadline - System.nanoTime());
      }
    }
  }

  private void throttleNanos(long startTimeNanos) {
    //throttle the operations
    if (targetOpsPerMs > 0) {
      // delay until next tick
      long deadline = startTimeNanos + opsdone * targetOpsTickNs;
      sleepUntil(deadline);
      measurements.setIntendedStartTimeNs(deadline);
    }
  }

  /**
   * The total amount of work this thread is still expected to do.
   */
  int getOpsTodo() {
    int todo = opcount - opsdone;
    return todo < 0 ? 0 : todo;
  }
}


/**
 * Main class for executing MSBench.
 */
public final class Client {

  private Client() {
    //not used
  }

  /**
   * The MESSAGE_SIZE class to be used.
   */
  public static final String MS_PROPERTY = "sys";

  /**
   * The exporter class to be used. The default is
   * com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter.
   */
  public static final String EXPORTER_PROPERTY = "exporter";

  /**
   * If set to the path of a file, YCSB will write all output to this file
   * instead of STDOUT.
   */
  public static final String EXPORT_FILE_PROPERTY = "exportfile";

  /**
   * Whether or not to show status during run.
   */
  public static final String STATUS_PROPERTY = "status";

  /**
   * An optional thread used to track progress and measure JVM stats.
   */
  private static StatusThread statusthread = null;


  public static void usageMessage() {

    System.out.println("Usage: java cn.ac.ict.MSClient [options]");
    System.out.println("Options:");
    System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n" +
            "        \"threadcount\" property using -p");
    System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
            "       be specified as the \"target\" property using -p");
    System.out.println("  -load:  run the loading phase of the workload");
    System.out.println("  -t:  run the transactions phase of the workload (default)");
    System.out.println("  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n" +
            "        can also be specified as the \"db\" property using -p");
    System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
    System.out.println("           be specified, and will be processed in the order specified");
    System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
    System.out.println("          multiple properties can be specified, and override any");
    System.out.println("          values in the propertyfile");
    System.out.println("  -s:  show status during run (default: no status)");
    System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
    System.out.println("");
    System.out.println("Required properties:");
    System.out.println("  " + WORKLOAD_PROPERTY + ": the name of the workload class to use (e.g. " +
            "com.yahoo.ycsb.workloads.CoreWorkload)");
    System.out.println("");
    System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
    System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
    System.out.println("use the \"insertcount\" and \"insertstart\" properties to divide up the records " +
            "to be inserted");
  }

  public static boolean checkRequiredProperties(Properties props) {
    return false;
  }


  /**
   * Exports the measurements to either sysout or a file using the exporter
   * loaded from conf.
   *
   * @throws IOException Either failed to write to output stream or failed to close it.
   */
  private static void exportMeasurements(Properties props, int opcount, long runtime)
          throws IOException {
    MeasurementsExporter exporter = null;
    try {
      // if no destination file is provided the results will be written to stdout
      OutputStream out;
      String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile == null) {
        out = System.out;
      } else {
        out = new FileOutputStream(exportFile);
      }

      // if no exporter is provided the default text one will be used
      String exporterStr = props.getProperty(EXPORTER_PROPERTY,
              "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
      try {
        exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class)
                .newInstance(out);
      } catch (Exception e) {
        System.err.println("Could not find exporter " + exporterStr
                + ", will use default text reporter.");
        e.printStackTrace();
        exporter = new TextMeasurementsExporter(out);
      }

      exporter.write("OVERALL", "RunTime(ms)", runtime);
      double throughput = 1000.0 * (opcount) / (runtime);
      exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

      final Map<String, Long[]> gcs = Utils.getGCStatst();
      long totalGCCount = 0;
      long totalGCTime = 0;
      for (final Entry<String, Long[]> entry : gcs.entrySet()) {
        exporter.write("TOTAL_GCS_" + entry.getKey(), "Count", entry.getValue()[0]);
        exporter.write("TOTAL_GC_TIME_" + entry.getKey(), "Time(ms)", entry.getValue()[1]);
        exporter.write("TOTAL_GC_TIME_%_" + entry.getKey(), "Time(%)",
                ((double) entry.getValue()[1] / runtime) * (double) 100);
        totalGCCount += entry.getValue()[0];
        totalGCTime += entry.getValue()[1];
      }
      exporter.write("TOTAL_GCs", "Count", totalGCCount);

      exporter.write("TOTAL_GC_TIME", "Time(ms)", totalGCTime);
      exporter.write("TOTAL_GC_TIME_%", "Time(%)", ((double) totalGCTime / runtime) * (double) 100);

      if (statusthread != null && statusthread.trackJVMStats()) {
        exporter.write("MAX_MEM_USED", "MBs", statusthread.getMaxUsedMem());
        exporter.write("MIN_MEM_USED", "MBs", statusthread.getMinUsedMem());
        exporter.write("MAX_THREADS", "Count", statusthread.getMaxThreads());
        exporter.write("MIN_THREADS", "Count", statusthread.getMinThreads());
        exporter.write("MAX_SYS_LOAD_AVG", "Load", statusthread.getMaxLoadAvg());
        exporter.write("MIN_SYS_LOAD_AVG", "Load", statusthread.getMinLoadAvg());
      }

      Measurements.getMeasurements().exportMeasurements(exporter);
    } finally {
      if (exporter != null) {
        exporter.close();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    ArgumentParser parser = argParser();
    try {
      Namespace res = parser.parseArgs(args);
      //TODO 检查若w和r都没有则抛异常
      //TODO 解析Hosts


    } catch (ArgumentParserException e) {
      if (args.length == 0) {
        parser.printHelp();
        System.exit(0);
      } else {
        parser.handleError(e);
        System.exit(1);
      }
    }




    // -------------------------------------------------------------------------------------
    Properties props = parseArguments(args);

    boolean status = Boolean.valueOf(props.getProperty(STATUS_PROPERTY, String.valueOf(false)));
    String label = props.getProperty(LABEL_PROPERTY, "");

    long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

    //get number of threads, target and db
    int threadcount = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
    String dbname = props.getProperty(DB_PROPERTY, "com.yahoo.ycsb.BasicDB");
    int target = Integer.parseInt(props.getProperty(TARGET_PROPERTY, "0"));

    //compute the target throughput
    double targetperthreadperms = -1;
    if (target > 0) {
      double targetperthread = ((double) target) / ((double) threadcount);
      targetperthreadperms = targetperthread / 1000.0;
    }

    Thread warningthread = setupWarningThread();
    warningthread.start();

    Measurements.setProperties(props);

    Workload workload = getWorkload(props);

    final Tracer tracer = getTracer(props, workload);

    initWorkload(props, warningthread, workload, tracer);

    System.err.println("Starting test.");
    final CountDownLatch completeLatch = new CountDownLatch(threadcount);

    final List<ClientThread> clients = initDb(dbname, props, threadcount, targetperthreadperms,
            workload, tracer, completeLatch);

    if (status) {
      boolean standardstatus = false;
      if (props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "").compareTo("timeseries") == 0) {
        standardstatus = true;
      }
      int statusIntervalSeconds = Integer.parseInt(props.getProperty("status.interval", "10"));
      boolean trackJVMStats = props.getProperty(Measurements.MEASUREMENT_TRACK_JVM_PROPERTY,
              Measurements.MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT).equals("true");
      statusthread = new StatusThread(completeLatch, clients, label, standardstatus, statusIntervalSeconds,
              trackJVMStats);
      statusthread.start();
    }

    Thread terminator = null;
    long st;
    long en;
    int opsDone;

    try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_SPAN)) {

      final Map<Thread, ClientThread> threads = new HashMap<>(threadcount);
      for (ClientThread client : clients) {
        threads.put(new Thread(tracer.wrap(client, "ClientThread")), client);
      }

      st = System.currentTimeMillis();

      for (Thread t : threads.keySet()) {
        t.start();
      }

      if (maxExecutionTime > 0) {
        terminator = new TerminatorThread(maxExecutionTime, threads.keySet(), workload);
        terminator.start();
      }

      opsDone = 0;

      for (Entry<Thread, ClientThread> entry : threads.entrySet()) {
        try {
          entry.getKey().join();
          opsDone += entry.getValue().getOpsDone();
        } catch (InterruptedException ignored) {
          // ignored
        }
      }

      en = System.currentTimeMillis();
    }

    try {
      try (final TraceScope span = tracer.newScope(CLIENT_CLEANUP_SPAN)) {

        if (terminator != null && !terminator.isInterrupted()) {
          terminator.interrupt();
        }

        if (status) {
          // wake up status thread if it's asleep
          statusthread.interrupt();
          // at this point we assume all the monitored threads are already gone as per above join loop.
          try {
            statusthread.join();
          } catch (InterruptedException ignored) {
            // ignored
          }
        }

        workload.cleanup();
      }
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      try (final TraceScope span = tracer.newScope(CLIENT_EXPORT_MEASUREMENTS_SPAN)) {
        exportMeasurements(props, opsDone, en - st);
      }
    } catch (IOException e) {
      System.err.println("Could not export measurements, error: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }

    System.exit(0);
  }


    // 用来初始化MS
    private static List<ClientThread> initMS(String dbname, Properties props, int threadcount,
                                             double targetperthreadperms, Workload workload
                                           CountDownLatch completeLatch) {

        final List<ClientThread> clients = new ArrayList<>(threadcount);
        try (final TraceScope span = tracer.newScope(CLIENT_INIT_SPAN)) {
          int opcount;

          DB db;
          try {
            db = MSFactory.newDB(dbname, props, tracer);
          } catch (UnknownDBException e) {
            System.out.println("Unknown DB " + dbname);
            initFailed = true;
            break;
          }

          int threadopcount = opcount / threadcount;

          // ensure correct number of operations, in case opcount is not a multiple of threadcount
          if (threadid < opcount % threadcount) {
            ++threadopcount;
          }

          ClientThread t = new ClientThread(db, dotransactions, workload, props, threadopcount, targetperthreadperms,
                  completeLatch);

          clients.add(t);
          if (initFailed) {
            System.err.println("Error initializing datastore bindings.");
            System.exit(0);
          }
        }
        return clients;
    }





  //TODO 后期需要一个自定义场景类

    /** Get the command-line argument parser. */
    //TODO 把参数改成静态变量
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("MSBench")
            .defaultHelp(true)
            .description("This tool is used to verify the MESSAGE_SIZE performance.");

        parser.addArgument(CONFIG_PRE + STREAM_NUM).action(store()).required(true).type(Integer.class).metavar(STREAM_NUM).dest(STREAM_NUM).help(STREAM_NUM_DOC);

        parser.addArgument(CONFIG_PRE + STREAM_NAME_PREFIX).action(store()).required(true).type(String.class).metavar(STREAM_NAME_PREFIX).dest(STREAM_NAME_PREFIX).help(STREAM_NAME_PREFIX_DOC);

        parser.addArgument(CONFIG_PRE + WRITER_NUM).action(store()).required(false).type(Integer.class).metavar(WRITER_NUM).dest(WRITER_NUM).help(WRITER_NUM_DOC);

        parser.addArgument(CONFIG_PRE + SYNC).action(store()).required(false).type(Integer.class).metavar(SYNC).dest(SYNC).help(SYNC_DOC);

        parser.addArgument(CONFIG_PRE + READER_NUM).action(store()).required(false).type(Integer.class).metavar(READER_NUM).dest(READER_NUM).help(READER_NUM_DOC);

        parser.addArgument(CONFIG_PRE + READ_FROM).action(store()).required(false).type(Integer.class).metavar(READ_FROM).dest(READ_FROM).help(READ_FROM_DOC);

        parser.addArgument(CONFIG_PRE + MESSAGE_SIZE).action(store()).required(true).type(Integer.class).metavar(MESSAGE_SIZE).help(MESSAGE_SIZE_DOC);

        parser.addArgument(CONFIG_PRE + RUN_TIME).action(store()).required(true).type(Integer.class).metavar(RUN_TIME).help(RUN_TIME_DOC);

        parser.addArgument(CONFIG_PRE + HOSTS).action(store()).required(true).type(String.class).metavar(HOSTS).help(HOSTS_DOC);

        parser.addArgument(CONFIG_PRE + THROUGHPUT).action(store()).required(false).type(Integer.class).metavar(THROUGHPUT).dest(THROUGHPUT).help(THROUGHPUT_DOC);

        parser.addArgument(CONFIG_PRE + FINAL_THROUGHPUT).action(store()).required(false).type(Integer.class).metavar(FINAL_THROUGHPUT).dest(FINAL_THROUGHPUT).help(FINAL_THROUGHPUT_DOC);

        parser.addArgument(CONFIG_PRE + CHANGE_THROUGHPUT).action(store()).required(false).type(Integer.class).metavar(CHANGE_THROUGHPUT).dest(CHANGE_THROUGHPUT).help(CHANGE_THROUGHPUT_DOC);

        parser.addArgument(CONFIG_PRE + CHANGE_THROUGHPUT_SECONDS).action(store()).required(false).type(Integer.class).metavar(CHANGE_THROUGHPUT_SECONDS).dest(CHANGE_THROUGHPUT_SECONDS).help(CHANGE_THROUGHPUT_SECONDS_DOC);

        parser.addArgument(CONFIG_PRE + RANDOM_THROUGHPUT_LIST).action(store()).required(false).type(String.class).metavar(RANDOM_THROUGHPUT_LIST).dest(RANDOM_THROUGHPUT_LIST).help(RANDOM_THROUGHPUT_LIST_DOC);

        parser.addArgument(CONFIG_PRE + CONFIG_FILE).action(store()).required(false).type(String.class).metavar(CONFIG_FILE).dest(CONFIG_FILE).help(CONFIG_FILE_DOC);

        return parser;
    }
}
