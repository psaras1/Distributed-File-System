import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class TestDistributedSystem {
  private static final String CLIENT_JAR = "client.jar";
  private static final int CONTROLLER_PORT = 12345;
  private static final int REPLICATION_FACTOR = 3;
  private static final int TIMEOUT = 3000;
  private static final int REBALANCE_PERIOD = 10;
  private static final int BASE_DSTORE_PORT = 13000;

  private Process controller;
  private List<Process> dstores = new ArrayList<>();
  private List<Integer> dstorePorts = new ArrayList<>();
  private Random random = new Random();

  public static void main(String[] args) {
    TestDistributedSystem tester = new TestDistributedSystem();
    try {
      tester.runAllTests();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tester.cleanup();
    }
  }

  public void runAllTests() throws Exception {
    System.out.println("=== STARTING DISTRIBUTED SYSTEM TESTS ===");

    // Start controller
    startController();
    Thread.sleep(2000); // Give controller time to start

    // Test 1: Basic protocol compliance
    System.out.println("\n=== TEST 1: BASIC PROTOCOL COMPLIANCE ===");
    startDstores(REPLICATION_FACTOR);
    Thread.sleep(5000); // Wait for all Dstores to join
    testBasicOperations();

    // Test 2: Replication
    System.out.println("\n=== TEST 2: CHECKING REPLICATION ===");
    testReplication();

    // Test 3: Concurrent requests
    System.out.println("\n=== TEST 3: CONCURRENT REQUESTS ===");
    testConcurrentRequests();

    // Test 4: Single Dstore failure
    System.out.println("\n=== TEST 4: SINGLE DSTORE FAILURE ===");
    testSingleDstoreFailure();

    // Test 5: Multiple Dstore failures
    System.out.println("\n=== TEST 5: MULTIPLE DSTORE FAILURES ===");
    testMultipleDstoreFailures();

    // Test 6: File distribution after joins/failures
    System.out.println("\n=== TEST 6: FILE DISTRIBUTION AFTER JOINS/FAILURES ===");
    testFileDistribution();

    System.out.println("\n=== ALL TESTS COMPLETED ===");
  }

  private void startController() throws IOException {
    ProcessBuilder pb = new ProcessBuilder(
        "java", "Controller",
        String.valueOf(CONTROLLER_PORT),
        String.valueOf(REPLICATION_FACTOR),
        String.valueOf(TIMEOUT),
        String.valueOf(REBALANCE_PERIOD)
    );
    pb.redirectErrorStream(true);
    controller = pb.start();

    startOutputReader(controller, "Controller");
    System.out.println("Controller started on port " + CONTROLLER_PORT);
  }

  private void startDstores(int count) throws IOException {
    for (int i = 0; i < count; i++) {
      int port = BASE_DSTORE_PORT + i;
      dstorePorts.add(port);
      startDstore(port);
    }
  }

  private void startDstore(int port) throws IOException {
    // Create folder for this Dstore
    Files.createDirectories(Paths.get("dstore_" + port));

    ProcessBuilder pb = new ProcessBuilder(
        "java", "Dstore",
        String.valueOf(port),
        String.valueOf(CONTROLLER_PORT),
        String.valueOf(TIMEOUT),
        "dstore_" + port
    );
    pb.redirectErrorStream(true);
    Process dstore = pb.start();
    dstores.add(dstore);

    startOutputReader(dstore, "Dstore-" + port);
    System.out.println("Dstore started on port " + port);
  }

  private void startOutputReader(Process process, String prefix) {
    new Thread(() -> {
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          System.out.println(prefix + ": " + line);
        }
      } catch (IOException e) {
        System.err.println("Error reading output from " + prefix + ": " + e.getMessage());
      }
    }).start();
  }

  private void testBasicOperations() throws Exception {
    // Create test files
    createTestFile("test1.txt", 1000);
    createTestFile("test2.txt", 2000);

    // Store test files
    executeClientCommand("store test1.txt");
    executeClientCommand("store test2.txt");

    // List files
    String listOutput = executeClientCommand("list");
    System.out.println("List output: " + listOutput);

    // Load files
    String loadOutput1 = executeClientCommand("load test1.txt downloaded1.txt");
    System.out.println("Load output 1: " + loadOutput1);

    String loadOutput2 = executeClientCommand("load test2.txt downloaded2.txt");
    System.out.println("Load output 2: " + loadOutput2);

    // Remove file
    String removeOutput = executeClientCommand("remove test1.txt");
    System.out.println("Remove output: " + removeOutput);

    // List again to verify removal
    listOutput = executeClientCommand("list");
    System.out.println("List after removal: " + listOutput);
  }

  private void testReplication() throws Exception {
    // Store a file
    createTestFile("replication_test.txt", 3000);
    executeClientCommand("store replication_test.txt");

    // Check in how many Dstores the file exists
    int count = 0;
    for (int port : dstorePorts) {
      if (Files.exists(Paths.get("dstore_" + port, "replication_test.txt"))) {
        count++;
      }
    }

    System.out.println("File is replicated in " + count + " Dstores (expected: " + REPLICATION_FACTOR + ")");
  }

  private void testConcurrentRequests() throws Exception {
    // Create test files
    for (int i = 1; i <= 5; i++) {
      createTestFile("concurrent" + i + ".txt", 1000 + i * 100);
    }

    // Launch concurrent store operations
    ExecutorService executor = Executors.newFixedThreadPool(5);
    List<Future<String>> futures = new ArrayList<>();

    for (int i = 1; i <= 5; i++) {
      int fileNum = i;
      futures.add(executor.submit(() -> executeClientCommand("store concurrent" + fileNum + ".txt")));
    }

    // Wait for all operations to complete
    for (Future<String> future : futures) {
      future.get();
    }

    // Check if all files are listed
    String listOutput = executeClientCommand("list");
    System.out.println("List after concurrent stores: " + listOutput);

    // Concurrent loads
    futures.clear();
    for (int i = 1; i <= 5; i++) {
      int fileNum = i;
      futures.add(executor.submit(() ->
          executeClientCommand("load concurrent" + fileNum + ".txt downloaded_concurrent" + fileNum + ".txt")));
    }

    // Wait for all operations to complete
    for (Future<String> future : futures) {
      future.get();
    }

    executor.shutdown();
  }

  private void testSingleDstoreFailure() throws Exception {
    // Store a file
    createTestFile("failure_test.txt", 4000);
    executeClientCommand("store failure_test.txt");

    // Kill one Dstore
    if (!dstores.isEmpty()) {
      Process dstore = dstores.remove(0);
      int port = dstorePorts.remove(0);
      dstore.destroy();
      System.out.println("Killed Dstore on port " + port);
      Thread.sleep(5000); // Give system time to detect failure
    }

    // Check if file can still be loaded
    String loadOutput = executeClientCommand("load failure_test.txt downloaded_after_failure.txt");
    System.out.println("Load after failure: " + loadOutput);

    // Start a new Dstore to replace the failed one
    int newPort = BASE_DSTORE_PORT + dstorePorts.size();
    startDstore(newPort);
    dstorePorts.add(newPort);
    Thread.sleep(15000); // Wait for rebalance
  }

  private void testMultipleDstoreFailures() throws Exception {
    // Store a file
    createTestFile("multiple_failures.txt", 5000);
    executeClientCommand("store multiple_failures.txt");

    // Kill multiple Dstores (up to N-R)
    int toKill = Math.min(dstores.size() - REPLICATION_FACTOR, dstores.size() - 1);
    if (toKill > 0) {
      for (int i = 0; i < toKill; i++) {
        Process dstore = dstores.remove(0);
        int port = dstorePorts.remove(0);
        dstore.destroy();
        System.out.println("Killed Dstore on port " + port);
      }
      Thread.sleep(5000); // Give system time to detect failures
    }

    // Check if file can still be loaded
    String loadOutput = executeClientCommand("load multiple_failures.txt downloaded_after_multiple.txt");
    System.out.println("Load after multiple failures: " + loadOutput);

    // Start new Dstores to replace the failed ones
    for (int i = 0; i < toKill; i++) {
      int newPort = BASE_DSTORE_PORT + dstorePorts.size() + 10;
      startDstore(newPort);
      dstorePorts.add(newPort);
    }

    Thread.sleep(15000); // Wait for rebalance
  }

  private void testFileDistribution() throws Exception {
    // Store multiple files
    for (int i = 1; i <= 10; i++) {
      createTestFile("dist" + i + ".txt", 1000 + i * 100);
      executeClientCommand("store dist" + i + ".txt");
    }

    // Check distribution across Dstores
    Map<Integer, Integer> fileCountMap = new HashMap<>();
    for (int port : dstorePorts) {
      try {
        int count = Files.list(Paths.get("dstore_" + port))
            .filter(Files::isRegularFile)
            .map(Path::getFileName)
            .map(Path::toString)
            .filter(name -> name.startsWith("dist"))
            .toArray().length;

        fileCountMap.put(port, count);
      } catch (IOException e) {
        System.err.println("Error counting files in Dstore_" + port + ": " + e.getMessage());
      }
    }

    // Print distribution
    System.out.println("File distribution across Dstores:");
    for (Map.Entry<Integer, Integer> entry : fileCountMap.entrySet()) {
      System.out.println("Dstore " + entry.getKey() + ": " + entry.getValue() + " files");
    }

    // Calculate standard deviation to measure evenness
    double mean = fileCountMap.values().stream().mapToInt(Integer::intValue).average().orElse(0);
    double variance = fileCountMap.values().stream()
        .mapToDouble(count -> Math.pow(count - mean, 2))
        .average().orElse(0);
    double stdDev = Math.sqrt(variance);

    System.out.println("Distribution statistics:");
    System.out.println("  Mean files per Dstore: " + mean);
    System.out.println("  Standard deviation: " + stdDev);
    System.out.println("  Lower is better, should be close to 0 for even distribution");
  }

  private void createTestFile(String filename, int size) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(filename)) {
      byte[] data = new byte[size];
      random.nextBytes(data);
      fos.write(data);
    }
    System.out.println("Created test file: " + filename + " (" + size + " bytes)");
  }

  private String executeClientCommand(String command) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(
        "java", "-cp", CLIENT_JAR + ":.", "ClientMain",
        String.valueOf(CONTROLLER_PORT),
        String.valueOf(TIMEOUT),
        command
    );

    Process process = pb.start();
    StringBuilder output = new StringBuilder();

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append("\n");
      }
    }

    process.waitFor(30, TimeUnit.SECONDS);
    return output.toString();
  }

  private void cleanup() {
    // Kill all processes
    if (controller != null) {
      controller.destroy();
    }

    for (Process dstore : dstores) {
      dstore.destroy();
    }

    // Clean up test files
    try {
      Files.list(Paths.get("."))
          .filter(path -> path.toString().endsWith(".txt"))
          .forEach(path -> {
            try {
              Files.delete(path);
            } catch (IOException e) {
              System.err.println("Failed to delete " + path + ": " + e.getMessage());
            }
          });

      // Clean up Dstore folders
      for (int port : dstorePorts) {
        Path folder = Paths.get("dstore_" + port);
        if (Files.exists(folder)) {
          Files.walk(folder)
              .sorted(Comparator.reverseOrder())
              .forEach(path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  System.err.println("Failed to delete " + path + ": " + e.getMessage());
                }
              });
        }
      }
    } catch (IOException e) {
      System.err.println("Error during cleanup: " + e.getMessage());
    }
  }
}