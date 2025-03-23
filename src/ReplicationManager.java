import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ReplicationManager is responsible for managing the replication of files
 * across Dstores in the distributed file system.
 */
public class ReplicationManager {

  // Replication factor - how many Dstores should each file be stored on
  private final int replicationFactor;

  // Timeout for operations in milliseconds
  private final int timeout;

  // Reference to the file index
  private final Index index;

  // Map of which files are stored on which Dstores
  private final Map<String, Set<Integer>> fileToDstores;

  // Map of which files each Dstore contains
  private final Map<Integer, Set<String>> dstoreToFiles;

  // Track store acknowledgments from Dstores
  private final Map<String, StoreOperationTracker> storeOperations;

  // Track remove acknowledgments from Dstores
  private final Map<String, RemoveOperationTracker> removeOperations;

  // Locks for thread safety
  private final ReentrantReadWriteLock fileDstoresLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock operationsLock = new ReentrantReadWriteLock();

  /**
   * Creates a new ReplicationManager.
   *
   * @param replicationFactor Number of Dstores to replicate each file on
   * @param timeout Timeout for operations in milliseconds
   * @param index Reference to the file index
   */
  public ReplicationManager(int replicationFactor, int timeout, Index index) {
    this.replicationFactor = replicationFactor;
    this.timeout = timeout;
    this.index = index;

    this.fileToDstores = new ConcurrentHashMap<>();
    this.dstoreToFiles = new ConcurrentHashMap<>();
    this.storeOperations = new ConcurrentHashMap<>();
    this.removeOperations = new ConcurrentHashMap<>();
  }

  /**
   * Selects R Dstores for storing a file, ensuring even distribution.
   *
   * @param availableDstores Set of available Dstore ports
   * @return List of selected Dstore ports, or empty list if not enough Dstores
   */
  public List<Integer> selectDstoresForStore(Set<Integer> availableDstores) {
    if (availableDstores.size() < replicationFactor) {
      return new ArrayList<>();
    }

    // Create a map of Dstore -> number of files stored
    Map<Integer, Integer> dstoreLoads = new HashMap<>();

    fileDstoresLock.readLock().lock();
    try {
      // Initialize all available Dstores with current file counts
      for (int dstore : availableDstores) {
        Set<String> files = dstoreToFiles.getOrDefault(dstore, new HashSet<>());
        dstoreLoads.put(dstore, files.size());
      }
    } finally {
      fileDstoresLock.readLock().unlock();
    }

    // Sort Dstores by load (number of files)
    List<Map.Entry<Integer, Integer>> sortedDstores = new ArrayList<>(dstoreLoads.entrySet());
    sortedDstores.sort(Map.Entry.comparingByValue());

    // Select the R least loaded Dstores
    List<Integer> selectedDstores = new ArrayList<>();
    for (int i = 0; i < replicationFactor && i < sortedDstores.size(); i++) {
      selectedDstores.add(sortedDstores.get(i).getKey());
    }

    return selectedDstores;
  }

  /**
   * Begins a store operation by tracking which Dstores need to acknowledge.
   *
   * @param filename The name of the file being stored
   * @param filesize The size of the file in bytes
   * @param selectedDstores The Dstores selected for storing the file
   * @param clientSocket The socket of the client that initiated the operation
   */
  public void beginStoreOperation(String filename, int filesize, List<Integer> selectedDstores, Socket clientSocket) {
    operationsLock.writeLock().lock();
    try {
      // Create a new store operation tracker
      StoreOperationTracker tracker = new StoreOperationTracker(
          filename, filesize, new HashSet<>(selectedDstores), clientSocket);
      storeOperations.put(filename, tracker);

      // Start timeout timer for this operation
      startStoreTimeout(filename);
    } finally {
      operationsLock.writeLock().unlock();
    }
  }

  /**
   * Handles a store acknowledgment from a Dstore.
   *
   * @param filename The name of the file
   * @param dstorePort The port of the Dstore that sent the acknowledgment
   * @return true if all expected acknowledgments have been received, false otherwise
   */
  public boolean handleStoreAck(String filename, int dstorePort) {
    boolean allAcksReceived = false;

    operationsLock.writeLock().lock();
    try {
      // Always update the tracking maps, regardless of tracker state
      updateFileLocation(filename, dstorePort);

      StoreOperationTracker tracker = storeOperations.get(filename);
      if (tracker != null && tracker.expectedDstores.contains(dstorePort)) {
        // Record the acknowledgment
        tracker.acknowledgedDstores.add(dstorePort);

        // Check if all acknowledgments have been received
        allAcksReceived = tracker.acknowledgedDstores.equals(tracker.expectedDstores);

        if (allAcksReceived) {
          tracker.isComplete = true;
        }
      }
    } finally {
      operationsLock.writeLock().unlock();
    }

    return allAcksReceived;
  }

  /**
   * Updates mappings to record that a file is stored on a specific Dstore.
   *
   * @param filename The name of the file
   * @param dstorePort The port of the Dstore
   */
  private void updateFileLocation(String filename, int dstorePort) {
    fileDstoresLock.writeLock().lock();
    try {
      // Update file -> Dstores mapping
      Set<Integer> dstores = fileToDstores.computeIfAbsent(filename, k -> new HashSet<>());
      dstores.add(dstorePort);

      // Update Dstore -> files mapping
      Set<String> files = dstoreToFiles.computeIfAbsent(dstorePort, k -> new HashSet<>());
      files.add(filename);
    } finally {
      fileDstoresLock.writeLock().unlock();
    }
  }

  /**
   * Starts a timeout timer for a store operation.
   *
   * @param filename The name of the file
   */
  private void startStoreTimeout(String filename) {
    Timer timer = new Timer(true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        handleStoreTimeout(filename);
      }
    }, timeout);
  }

  /**
   * Handles a timeout for a store operation.
   *
   * @param filename The name of the file
   */
  private void handleStoreTimeout(String filename) {operationsLock.writeLock().lock();
    try {
      StoreOperationTracker tracker = storeOperations.get(filename);
      if (tracker != null && !tracker.isComplete) {
        // Operation timed out, clean up
        storeOperations.remove(filename);

        // Remove any partial file locations
        fileDstoresLock.writeLock().lock();
        try {
          Set<Integer> dstores = fileToDstores.remove(filename);
          if (dstores != null) {
            for (int dstore : dstores) {
              Set<String> files = dstoreToFiles.get(dstore);
              if (files != null) {
                files.remove(filename);
              }
            }
          }
        } finally {
          fileDstoresLock.writeLock().unlock();
        }
      }
    } finally {
      operationsLock.writeLock().unlock();
    }
  }

  /**
   * Completes a store operation after all acknowledgments have been received.
   *
   * @param filename The name of the file
   */
  public void completeStoreOperation(String filename) {
    operationsLock.writeLock().lock();
    try {
      storeOperations.remove(filename);
    } finally {
      operationsLock.writeLock().unlock();
    }
  }

  /**
   * Gets the client socket for a store operation.
   *
   * @param filename The name of the file
   * @return The client socket, or null if not found
   */
  public Socket getStoreClientSocket(String filename) {
    operationsLock.readLock().lock();
    try {
      StoreOperationTracker tracker = storeOperations.get(filename);
      return tracker != null ? tracker.clientSocket : null;
    } finally {
      operationsLock.readLock().unlock();
    }
  }

  /**
   * Begins a remove operation by tracking which Dstores need to acknowledge.
   *
   * @param filename The name of the file being removed
   * @param clientSocket The socket of the client that initiated the operation
   */
  public void beginRemoveOperation(String filename, Socket clientSocket) {
    Set<Integer> dstoresToRemoveFrom = new HashSet<>();

    fileDstoresLock.readLock().lock();
    try {
      Set<Integer> dstores = fileToDstores.get(filename);
      if (dstores != null) {
        dstoresToRemoveFrom.addAll(dstores);
      }
    } finally {
      fileDstoresLock.readLock().unlock();
    }

    operationsLock.writeLock().lock();
    try {
      // Create a new remove operation tracker
      RemoveOperationTracker tracker = new RemoveOperationTracker(
          filename, dstoresToRemoveFrom, clientSocket);
      removeOperations.put(filename, tracker);

      // Start timeout timer for this operation
      startRemoveTimeout(filename);
    } finally {
      operationsLock.writeLock().unlock();
    }
  }

  /**
   * Gets the Dstores that have a specific file.
   *
   * @param filename The name of the file
   * @return Set of Dstore ports that have the file
   */
  public Set<Integer> getDstoresForFile(String filename) {
    fileDstoresLock.readLock().lock();
    try {
      Set<Integer> dstores = fileToDstores.get(filename);
      return dstores != null ? new HashSet<>(dstores) : new HashSet<>();
    } finally {
      fileDstoresLock.readLock().unlock();
    }
  }

  /**
   * Handles a remove acknowledgment from a Dstore.
   *
   * @param filename The name of the file
   * @param dstorePort The port of the Dstore that sent the acknowledgment
   * @return true if all expected acknowledgments have been received, false otherwise
   */
  public boolean handleRemoveAck(String filename, int dstorePort) {
    boolean allAcksReceived = false;

    operationsLock.writeLock().lock();
    try {
      RemoveOperationTracker tracker = removeOperations.get(filename);
      if (tracker != null && tracker.expectedDstores.contains(dstorePort)) {
        // Record the acknowledgment
        tracker.acknowledgedDstores.add(dstorePort);

        // Remove file location mapping
        removeFileLocation(filename, dstorePort);

        // Check if all acknowledgments have been received
        allAcksReceived = tracker.acknowledgedDstores.equals(tracker.expectedDstores);

        if (allAcksReceived) {
          // Operation is complete, remove from tracking
          // Don't remove yet, keep for potential timeout handling
          tracker.isComplete = true;
        }
      }
    } finally {
      operationsLock.writeLock().unlock();
    }

    return allAcksReceived;
  }

  /**
   * Updates mappings to record that a file has been removed from a specific Dstore.
   *
   * @param filename The name of the file
   * @param dstorePort The port of the Dstore
   */
  private void removeFileLocation(String filename, int dstorePort) {
    fileDstoresLock.writeLock().lock();
    try {
      // Update file -> Dstores mapping
      Set<Integer> dstores = fileToDstores.get(filename);
      if (dstores != null) {
        dstores.remove(dstorePort);
        if (dstores.isEmpty()) {
          fileToDstores.remove(filename);
        }
      }

      // Update Dstore -> files mapping
      Set<String> files = dstoreToFiles.get(dstorePort);
      if (files != null) {
        files.remove(filename);
        if (files.isEmpty()) {
          dstoreToFiles.remove(dstorePort);
        }
      }
    } finally {
      fileDstoresLock.writeLock().unlock();
    }
  }

  /**
   * Starts a timeout timer for a remove operation.
   *
   * @param filename The name of the file
   */
  private void startRemoveTimeout(String filename) {
    Timer timer = new Timer(true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        handleRemoveTimeout(filename);
      }
    }, timeout);
  }

  /**
   * Handles a timeout for a remove operation.
   *
   * @param filename The name of the file
   */
  private void handleRemoveTimeout(String filename) {
    operationsLock.writeLock().lock();
    try {
      RemoveOperationTracker tracker = removeOperations.get(filename);
      if (tracker != null && !tracker.isComplete) {
        // Operation timed out, but we leave the file in "remove in progress" state
        // Future rebalance operations will try to sort this out
        // Don't remove the tracker, as it's needed to track the client socket
      }
    } finally {
      operationsLock.writeLock().unlock();
    }
  }

  /**
   * Completes a remove operation after all acknowledgments have been received.
   *
   * @param filename The name of the file
   */
  public void completeRemoveOperation(String filename) {
    operationsLock.writeLock().lock();
    try {
      removeOperations.remove(filename);
    } finally {
      operationsLock.writeLock().unlock();
    }
  }

  /**
   * Gets the client socket for a remove operation.
   *
   * @param filename The name of the file
   * @return The client socket, or null if not found
   */
  public Socket getRemoveClientSocket(String filename) {
    operationsLock.readLock().lock();
    try {
      RemoveOperationTracker tracker = removeOperations.get(filename);
      return tracker != null ? tracker.clientSocket : null;
    } finally {
      operationsLock.readLock().unlock();
    }
  }

  /**
   * Handles the disconnection of a Dstore.
   *
   * @param dstorePort The port of the disconnected Dstore
   * @return Set of filenames that were stored on the disconnected Dstore
   */
  public Set<String> handleDstoreDisconnection(int dstorePort) {
    Set<String> affectedFiles = new HashSet<>();

    fileDstoresLock.writeLock().lock();
    try {
      // Get the files that were on this Dstore
      Set<String> files = dstoreToFiles.remove(dstorePort);

      if (files != null) {
        affectedFiles.addAll(files);

        // Update fileToDstores mappings
        for (String filename : files) {
          Set<Integer> dstores = fileToDstores.get(filename);
          if (dstores != null) {
            dstores.remove(dstorePort);

            // If a file is no longer stored on any Dstore, remove it
            if (dstores.isEmpty()) {
              fileToDstores.remove(filename);
            }
          }
        }
      }
    } finally {
      fileDstoresLock.writeLock().unlock();
    }

    return affectedFiles;
  }

  /**
   * Selects a Dstore for loading a file, trying to balance load.
   *
   * @param filename The name of the file
   * @param triedDstores Set of Dstore ports that have already been tried
   * @return The selected Dstore port, or -1 if no Dstore has the file
   */
  public int selectDstoreForLoad(String filename, Set<Integer> triedDstores) {
    fileDstoresLock.readLock().lock();
    try {
      Set<Integer> dstores = fileToDstores.get(filename);

      if (dstores == null || dstores.isEmpty()) {
        return -1;
      }

      // Filter out Dstores that have already been tried
      List<Integer> availableDstores = new ArrayList<>();
      for (int dstore : dstores) {
        if (!triedDstores.contains(dstore)) {
          availableDstores.add(dstore);
        }
      }

      if (availableDstores.isEmpty()) {
        // All Dstores have been tried, we start over
        return dstores.iterator().next();
      }

      // Return a random available Dstore
      return availableDstores.get(new Random().nextInt(availableDstores.size()));
    } finally {
      fileDstoresLock.readLock().unlock();
    }
  }

  /**
   * Gets all files stored in the system with their respective Dstores.
   *
   * @return Map of filename to set of Dstore ports
   */
  public Map<String, Set<Integer>> getAllFileLocations() {
    Map<String, Set<Integer>> result = new HashMap<>();

    fileDstoresLock.readLock().lock();
    try {
      for (Map.Entry<String, Set<Integer>> entry : fileToDstores.entrySet()) {
        result.put(entry.getKey(), new HashSet<>(entry.getValue()));
      }
    } finally {
      fileDstoresLock.readLock().unlock();
    }

    return result;
  }

  /**
   * Gets all files stored on a specific Dstore.
   *
   * @param dstorePort The port of the Dstore
   * @return Set of filenames stored on the Dstore
   */
  public Set<String> getFilesOnDstore(int dstorePort) {
    fileDstoresLock.readLock().lock();
    try {
      Set<String> files = dstoreToFiles.get(dstorePort);
      return files != null ? new HashSet<>(files) : new HashSet<>();
    } finally {
      fileDstoresLock.readLock().unlock();
    }
  }

  /**
   * Updates the system's view of which files are on which Dstores.
   * Used during rebalance operations.
   *
   * @param dstorePort The port of the Dstore
   * @param files Set of filenames on the Dstore
   */
  public void updateDstoreFiles(int dstorePort, Set<String> files) {
    fileDstoresLock.writeLock().lock();
    try {
      // Remove old mappings for this Dstore
      Set<String> oldFiles = dstoreToFiles.getOrDefault(dstorePort, new HashSet<>());

      // Files no longer on this Dstore
      Set<String> removedFiles = new HashSet<>(oldFiles);
      removedFiles.removeAll(files);

      // Files newly on this Dstore
      Set<String> newFiles = new HashSet<>(files);
      newFiles.removeAll(oldFiles);

      // Update fileToDstores for removed files
      for (String filename : removedFiles) {
        Set<Integer> dstores = fileToDstores.get(filename);
        if (dstores != null) {
          dstores.remove(dstorePort);
          if (dstores.isEmpty()) {
            fileToDstores.remove(filename);
          }
        }
      }

      // Update fileToDstores for new files
      for (String filename : newFiles) {
        Set<Integer> dstores = fileToDstores.computeIfAbsent(filename, k -> new HashSet<>());
        dstores.add(dstorePort);
      }

      // Update dstoreToFiles
      dstoreToFiles.put(dstorePort, new HashSet<>(files));
    } finally {
      fileDstoresLock.writeLock().unlock();
    }
  }

  /**
   * Calculates the rebalance operations needed to maintain R replicas
   * and even distribution of files.
   *
   * @param activeDstores List of active Dstore ports
   * @param dstoreFiles Map of Dstore port to set of filenames it has
   * @return Map of Dstore port to pair of files to send and files to remove
   */
  public Map<Integer, RebalanceOperation> calculateRebalance(List<Integer> activeDstores, Map<Integer, Set<String>> dstoreFiles) {
    if (activeDstores.size() < replicationFactor) {
      return new HashMap<>();
    }

    Map<Integer, RebalanceOperation> operations = new HashMap<>();
    Map<String, Set<Integer>> currentFileLocations = new HashMap<>();
    Set<String> allFiles = new HashSet<>();

    // Initialize operations for each Dstore
    for (Integer dstore : activeDstores) {
      operations.put(dstore, new RebalanceOperation());
    }

    // Build the current file locations map
    for (Map.Entry<Integer, Set<String>> entry : dstoreFiles.entrySet()) {
      int dstore = entry.getKey();
      Set<String> files = entry.getValue();

      for (String file : files) {
        Set<Integer> dstores = currentFileLocations.computeIfAbsent(file, k -> new HashSet<>());
        dstores.add(dstore);
        allFiles.add(file);
      }
    }

    // If no files, nothing to rebalance
    if (allFiles.isEmpty()) {
      return operations;
    }

    // Calculate target number of files per Dstore
    int totalFiles = allFiles.size();
    int totalFileReplicas = totalFiles * replicationFactor;
    int idealFilesPerDstore = totalFileReplicas / activeDstores.size();
    int remainder = totalFileReplicas % activeDstores.size();

    // Create a map of target file counts for each Dstore
    Map<Integer, Integer> targetFileCounts = new HashMap<>();
    for (int dstore : activeDstores) {
      // Distribute remainder evenly
      int target = idealFilesPerDstore + (remainder > 0 ? 1 : 0);
      if (remainder > 0) remainder--;
      targetFileCounts.put(dstore, target);
    }

    // Step 1: Ensure each file has R copies
    for (String file : allFiles) {
      Set<Integer> currentDstores = currentFileLocations.getOrDefault(file, new HashSet<>());

      if (currentDstores.size() < replicationFactor) {
        // File needs more replicas
        List<Integer> availableDstores = new ArrayList<>(activeDstores);
        availableDstores.removeAll(currentDstores);

        // Sort available Dstores by current load
        availableDstores.sort((d1, d2) -> {
          int load1 = dstoreFiles.getOrDefault(d1, new HashSet<>()).size();
          int load2 = dstoreFiles.getOrDefault(d2, new HashSet<>()).size();
          return Integer.compare(load1, load2);
        });

        // Add new replicas
        for (int i = 0; i < replicationFactor - currentDstores.size() && i < availableDstores.size(); i++) {
          int dstoreDest = availableDstores.get(i);

          // Find a source Dstore for this file
          int dstoreSource = currentDstores.iterator().next();

          // Update the rebalance operation for the source Dstore
          RebalanceOperation sourceOp = operations.get(dstoreSource);
          Map<String, List<Integer>> filesToSend = sourceOp.filesToSend;

          List<Integer> destinations = filesToSend.computeIfAbsent(file, k -> new ArrayList<>());
          destinations.add(dstoreDest);

          // Update our tracking
          currentDstores.add(dstoreDest);
          Set<String> dstoreFileSet = dstoreFiles.computeIfAbsent(dstoreDest, k -> new HashSet<>());
          dstoreFileSet.add(file);
        }
      } else if (currentDstores.size() > replicationFactor) {
        // File has too many replicas
        List<Integer> currentDstoresList = new ArrayList<>(currentDstores);

        // Sort Dstores by load in descending order
        currentDstoresList.sort((d1, d2) -> {
          int load1 = dstoreFiles.getOrDefault(d1, new HashSet<>()).size();
          int load2 = dstoreFiles.getOrDefault(d2, new HashSet<>()).size();
          return Integer.compare(load2, load1);
        });

        // Remove excess replicas
        for (int i = 0; i < currentDstores.size() - replicationFactor; i++) {
          int dstore = currentDstoresList.get(i);

          // Update the rebalance operation
          RebalanceOperation op = operations.get(dstore);
          op.filesToRemove.add(file);

          // Update our tracking
          currentDstores.remove(dstore);
          Set<String> dstoreFileSet = dstoreFiles.get(dstore);
          if (dstoreFileSet != null) {
            dstoreFileSet.remove(file);
          }
        }
      }
    }

    // Count current files per Dstore after replication fixes
    Map<Integer, Integer> currentFileCounts = new HashMap<>();
    for (int dstore : activeDstores) {
      Set<String> files = dstoreFiles.getOrDefault(dstore, new HashSet<>());
      currentFileCounts.put(dstore, files.size());
    }

    // Check if any Dstore is empty - we should always distribute files to all Dstores
    boolean hasEmptyDstores = false;
    for (int dstore : activeDstores) {
      if (currentFileCounts.getOrDefault(dstore, 0) == 0) {
        hasEmptyDstores = true;
        break;
      }
    }

    // Check if load is balanced
    int maxFiles = Collections.max(currentFileCounts.values());
    int minFiles = Collections.min(currentFileCounts.values());
    boolean isUnbalanced = (maxFiles - minFiles > 1) || hasEmptyDstores;

    // Step 2: Perform load balancing if needed
    if (isUnbalanced) {
      // Identify overloaded and underloaded Dstores
      List<Integer> overloadedDstores = new ArrayList<>();
      List<Integer> underloadedDstores = new ArrayList<>();

      for (int dstore : activeDstores) {
        int currentCount = currentFileCounts.getOrDefault(dstore, 0);
        int targetCount = targetFileCounts.get(dstore);

        if (currentCount > targetCount) {
          overloadedDstores.add(dstore);
        } else if (currentCount < targetCount) {
          underloadedDstores.add(dstore);
        }
      }

      // Sort overloaded Dstores by the number of excess files
      overloadedDstores.sort((d1, d2) -> {
        int excess1 = currentFileCounts.get(d1) - targetFileCounts.get(d1);
        int excess2 = currentFileCounts.get(d2) - targetFileCounts.get(d2);
        return Integer.compare(excess2, excess1);
      });

      // Sort underloaded Dstores by the number of missing files
      underloadedDstores.sort((d1, d2) -> {
        int deficit1 = targetFileCounts.get(d1) - currentFileCounts.get(d1);
        int deficit2 = targetFileCounts.get(d2) - currentFileCounts.get(d2);
        return Integer.compare(deficit2, deficit1);
      });

      // Move files from overloaded to underloaded Dstores
      for (int overloadedDstore : overloadedDstores) {
        if (underloadedDstores.isEmpty()) break;

        Set<String> files = new HashSet<>(dstoreFiles.getOrDefault(overloadedDstore, new HashSet<>()));
        int currentCount = files.size();
        int targetCount = targetFileCounts.get(overloadedDstore);

        if (currentCount <= targetCount) continue;

        // Prioritize files that are already well-replicated
        List<String> filesList = new ArrayList<>(files);
        filesList.sort((f1, f2) -> {
          int count1 = currentFileLocations.getOrDefault(f1, new HashSet<>()).size();
          int count2 = currentFileLocations.getOrDefault(f2, new HashSet<>()).size();
          return Integer.compare(count2, count1);
        });

        for (String file : filesList) {
          if (currentCount <= targetCount || underloadedDstores.isEmpty()) break;

          Set<Integer> fileDstores = currentFileLocations.get(file);
          if (fileDstores == null || fileDstores.size() < replicationFactor) continue;

          // Find underloaded Dstore that doesn't have this file
          for (int i = 0; i < underloadedDstores.size(); i++) {
            int underloadedDstore = underloadedDstores.get(i);

            if (!fileDstores.contains(underloadedDstore)) {
              // Can move this file to underloaded Dstore
              RebalanceOperation op = operations.get(overloadedDstore);
              List<Integer> destinations = op.filesToSend.computeIfAbsent(file, k -> new ArrayList<>());
              destinations.add(underloadedDstore);

              // After sending, remove from source
              op.filesToRemove.add(file);

              // Update our tracking
              fileDstores.remove(overloadedDstore);
              fileDstores.add(underloadedDstore);
              dstoreFiles.get(overloadedDstore).remove(file);
              dstoreFiles.computeIfAbsent(underloadedDstore, k -> new HashSet<>()).add(file);

              // Update counts
              currentCount--;
              int underloadedCount = dstoreFiles.get(underloadedDstore).size();
              currentFileCounts.put(overloadedDstore, currentCount);
              currentFileCounts.put(underloadedDstore, underloadedCount);

              // Check if underloaded Dstore is now balanced
              if (underloadedCount >= targetFileCounts.get(underloadedDstore)) {
                underloadedDstores.remove(i);
                i--;
              }

              break;
            }
          }
        }
      }
    }

    return operations;
  }

  /**
   * Class to track a store operation in progress.
   */
  private static class StoreOperationTracker {
    final String filename;
    final int filesize;
    final Set<Integer> expectedDstores;
    final Set<Integer> acknowledgedDstores;
    final Socket clientSocket;
    volatile boolean isComplete;

    public StoreOperationTracker(String filename, int filesize, Set<Integer> expectedDstores, Socket clientSocket) {
      this.filename = filename;
      this.filesize = filesize;
      this.expectedDstores = expectedDstores;
      this.acknowledgedDstores = new HashSet<>();
      this.clientSocket = clientSocket;
      this.isComplete = false;
    }
  }

  /**
   * Class to track a remove operation in progress.
   */
  private static class RemoveOperationTracker {
    final String filename;
    final Set<Integer> expectedDstores;
    final Set<Integer> acknowledgedDstores;
    final Socket clientSocket;
    volatile boolean isComplete;

    public RemoveOperationTracker(String filename, Set<Integer> expectedDstores, Socket clientSocket) {
      this.filename = filename;
      this.expectedDstores = expectedDstores;
      this.acknowledgedDstores = new HashSet<>();
      this.clientSocket = clientSocket;
      this.isComplete = false;
    }
  }

  /**
   * Class to represent a rebalance operation for a Dstore.
   */
  public static class RebalanceOperation {
    public final Map<String, List<Integer>> filesToSend;
    public final Set<String> filesToRemove;

    public RebalanceOperation() {
      this.filesToSend = new HashMap<>();
      this.filesToRemove = new HashSet<>();
    }
  }

  /**
   * Marks files as under-replicated for priority handling when more Dstores join.
   *
   * @param files Set of filenames that are under-replicated
   */
  public void markFilesAsUnderReplicated(Set<String> files) {
    // This set will be checked when new Dstores join
    Set<String> underReplicatedFiles = new HashSet<>(files);

    // Log current file locations for monitoring
    fileDstoresLock.readLock().lock();
    try {
      for (String file : files) {
        Set<Integer> locations = fileToDstores.getOrDefault(file, new HashSet<>());
        System.out.println("Under-replicated file: " + file + " currently on Dstores: " + locations);
      }
    } finally {
      fileDstoresLock.readLock().unlock();
    }
  }
}