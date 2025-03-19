import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The Index class maintains a data structure for tracking files in the distributed storage system.
 */
public class Index {

  // Possible file states
  private enum FileState {
    STORE_IN_PROGRESS,
    STORE_COMPLETE,
    REMOVE_IN_PROGRESS
  }

  // File information
  private class FileInfo {
    String filename;
    int size;
    FileState state;
    Socket clientSocket;  // Socket of the client that initiated the operation

    public FileInfo(String filename, int size, Socket clientSocket) {
      this.filename = filename;
      this.size = size;
      this.state = FileState.STORE_IN_PROGRESS;
      this.clientSocket = clientSocket;
    }
  }

  // Map from filename to file information
  private final Map<String, FileInfo> files;

  // Track which Dstores have been tried for load operations
  private final Map<Socket, Map<String, Set<Integer>>> triedDstores;

  // Locks for thread safety
  private final ReentrantReadWriteLock filesLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock triedDstoresLock = new ReentrantReadWriteLock();

  /**
   * Constructs a new Index.
   */
  public Index() {
    this.files = new ConcurrentHashMap<>();
    this.triedDstores = new ConcurrentHashMap<>();
  }

  /**
   * Adds a new file to the index in the STORE_IN_PROGRESS state.
   *
   * @param filename The name of the file
   * @param size The size of the file in bytes
   * @param clientSocket The socket of the client that initiated the store operation
   */
  public void addFile(String filename, int size, Socket clientSocket) {
    filesLock.writeLock().lock();
    try {
      files.put(filename, new FileInfo(filename, size, clientSocket));
    } finally {
      filesLock.writeLock().unlock();
    }
  }

  /**
   * Checks if a file exists in the index (in any state).
   *
   * @param filename The name of the file to check
   * @return true if the file exists in the index, false otherwise
   */
  public boolean fileExists(String filename) {
    filesLock.readLock().lock();
    try {
      return files.containsKey(filename);
    } finally {
      filesLock.readLock().unlock();
    }
  }

  /**
   * Checks if a file is in the STORE_COMPLETE state.
   *
   * @param filename The name of the file to check
   * @return true if the file is in the STORE_COMPLETE state, false otherwise
   */
  public boolean isStoreComplete(String filename) {
    filesLock.readLock().lock();
    try {
      FileInfo fileInfo = files.get(filename);
      return fileInfo != null && fileInfo.state == FileState.STORE_COMPLETE;
    } finally {
      filesLock.readLock().unlock();
    }
  }

  /**
   * Checks if a file is in the REMOVE_IN_PROGRESS state.
   *
   * @param filename The name of the file to check
   * @return true if the file is in the REMOVE_IN_PROGRESS state, false otherwise
   */
  public boolean isRemoveInProgress(String filename) {
    filesLock.readLock().lock();
    try {
      FileInfo fileInfo = files.get(filename);
      return fileInfo != null && fileInfo.state == FileState.REMOVE_IN_PROGRESS;
    } finally {
      filesLock.readLock().unlock();
    }
  }

  /**
   * Gets the size of a file.
   *
   * @param filename The name of the file
   * @return The size of the file in bytes, or -1 if the file doesn't exist
   */
  public int getFileSize(String filename) {
    filesLock.readLock().lock();
    try {
      FileInfo fileInfo = files.get(filename);
      return fileInfo != null ? fileInfo.size : -1;
    } finally {
      filesLock.readLock().unlock();
    }
  }

  /**
   * Updates a file's state to STORE_COMPLETE.
   *
   * @param filename The name of the file
   */
  public void completeStoreOperation(String filename) {
    filesLock.writeLock().lock();
    try {
      FileInfo fileInfo = files.get(filename);

      if (fileInfo != null && fileInfo.state == FileState.STORE_IN_PROGRESS) {
        fileInfo.state = FileState.STORE_COMPLETE;
        fileInfo.clientSocket = null;  // Clear client socket reference
      }
    } finally {
      filesLock.writeLock().unlock();
    }
  }

  /**
   * Marks a file for removal.
   *
   * @param filename The name of the file
   * @param clientSocket The socket of the client that initiated the remove operation
   */
  public void startRemoveOperation(String filename, Socket clientSocket) {
    filesLock.writeLock().lock();
    try {
      FileInfo fileInfo = files.get(filename);

      if (fileInfo != null && fileInfo.state == FileState.STORE_COMPLETE) {
        fileInfo.state = FileState.REMOVE_IN_PROGRESS;
        fileInfo.clientSocket = clientSocket;
      }
    } finally {
      filesLock.writeLock().unlock();
    }
  }

  /**
   * Removes a file from the index.
   *
   * @param filename The name of the file
   */
  public void completeRemoveOperation(String filename) {
    filesLock.writeLock().lock();
    try {
      files.remove(filename);
    } finally {
      filesLock.writeLock().unlock();
    }
  }

  /**
   * Gets a list of all files in the STORE_COMPLETE state.
   *
   * @return A list of filenames
   */
  public List<String> getCompleteFiles() {
    List<String> completeFiles = new ArrayList<>();

    filesLock.readLock().lock();
    try {
      for (Map.Entry<String, FileInfo> entry : files.entrySet()) {
        if (entry.getValue().state == FileState.STORE_COMPLETE) {
          completeFiles.add(entry.getKey());
        }
      }
    } finally {
      filesLock.readLock().unlock();
    }

    return completeFiles;
  }

  /**
   * Gets a list of all files in the index (in any state).
   *
   * @return A list of filenames
   */
  public List<String> getAllFiles() {
    List<String> allFiles = new ArrayList<>();

    filesLock.readLock().lock();
    try {
      allFiles.addAll(files.keySet());
    } finally {
      filesLock.readLock().unlock();
    }

    return allFiles;
  }

  /**
   * Sets the Dstores that have been tried for a load operation.
   *
   * @param clientSocket The socket of the client
   * @param filename The name of the file
   * @param triedDstoreSet Set of Dstore ports that have been tried
   */
  public void setTriedDstores(Socket clientSocket, String filename, Set<Integer> triedDstoreSet) {
    triedDstoresLock.writeLock().lock();
    try {
      Map<String, Set<Integer>> fileMap = triedDstores.computeIfAbsent(clientSocket, k -> new HashMap<>());
      fileMap.put(filename, new HashSet<>(triedDstoreSet));
    } finally {
      triedDstoresLock.writeLock().unlock();
    }
  }

  /**
   * Gets the Dstores that have been tried for a load operation.
   *
   * @param clientSocket The socket of the client
   * @param filename The name of the file
   * @return Set of Dstore ports that have been tried, or an empty set if not tracking
   */
  public Set<Integer> getTriedDstores(Socket clientSocket, String filename) {
    triedDstoresLock.readLock().lock();
    try {
      Map<String, Set<Integer>> fileMap = triedDstores.get(clientSocket);
      if (fileMap != null) {
        Set<Integer> tried = fileMap.get(filename);
        return tried != null ? new HashSet<>(tried) : new HashSet<>();
      }
      return new HashSet<>();
    } finally {
      triedDstoresLock.readLock().unlock();
    }
  }

  /**
   * Removes tracking of tried Dstores for a client.
   *
   * @param clientSocket The socket of the client
   */
  public void removeTriedDstores(Socket clientSocket) {
    triedDstoresLock.writeLock().lock();
    try {
      triedDstores.remove(clientSocket);
    } finally {
      triedDstoresLock.writeLock().unlock();
    }
  }
}