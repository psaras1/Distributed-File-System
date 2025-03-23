import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Controller {

  // Controller parameters
  private final int cport;
  private final int r;
  private final int timeout;
  private final int rebalancePeriod;

  // Connection management
  private ServerSocket serverSocket;
  private Map<Integer, DstoreInfo> dstores; // Port -> DstoreInfo mapping
  private final ReentrantReadWriteLock dstoresLock = new ReentrantReadWriteLock();

  // Index to track files and their states
  private Index index;

  // Replication manager to handle file replication
  private ReplicationManager replicationManager;

  // Timer for periodic rebalance operations
  private Timer rebalanceTimer;

  // Flag to indicate if a rebalance is currently in progress
  private volatile boolean rebalanceInProgress = false;

  // Queue for client requests during rebalance
  private final Queue<QueuedOperation> queuedOperations = new ConcurrentLinkedQueue<>();

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java Controller cport R timeout rebalance_period");
      System.exit(1);
    }

    int cport = Integer.parseInt(args[0]);
    int r = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    int rebalancePeriod = Integer.parseInt(args[3]);

    new Controller(cport, r, timeout, rebalancePeriod).start();
  }

  public Controller(int cport, int r, int timeout, int rebalancePeriod) {
    this.cport = cport;
    this.r = r;
    this.timeout = timeout;
    this.rebalancePeriod = rebalancePeriod;

    this.dstores = new ConcurrentHashMap<>();
    this.index = new Index();
    this.replicationManager = new ReplicationManager(r, timeout, index);
  }

  public void start() {
    try {
      // Create server socket to listen for connections
      serverSocket = new ServerSocket(cport);
      System.out.println("Controller started on port " + cport);

      // Schedule periodic rebalance operations
      scheduleRebalance();

      // Main loop to accept connections
      while (true) {
        Socket clientSocket = serverSocket.accept();
        // Handle the connection in a new thread
        new Thread(() -> handleConnection(clientSocket)).start();
      }
    } catch (IOException e) {
      System.err.println("Controller error: " + e.getMessage());
      e.printStackTrace();
    } finally {
      try {
        if (serverSocket != null) serverSocket.close();
      } catch (IOException e) {
        System.err.println("Error closing server socket: " + e.getMessage());
      }

      if (rebalanceTimer != null) {
        rebalanceTimer.cancel();
      }
    }
  }

  private void scheduleRebalance() {
    rebalanceTimer = new Timer(true);
    rebalanceTimer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        if (!rebalanceInProgress && getDstoreCount() >= r) {
          startRebalance();
        }
      }
    }, rebalancePeriod * 1000, rebalancePeriod * 1000);
  }

  private void handleConnection(Socket socket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      String firstMessage = in.readLine();

      if (firstMessage == null) {
        socket.close();
        return;
      }

      // Parse the first message to determine if it's from a Dstore or Client
      String[] tokens = firstMessage.split(" ");

      if (tokens[0].equals(Protocol.JOIN_TOKEN)) {
        // This is a Dstore connecting
        handleDstoreConnection(socket, firstMessage);
      } else {
        // This is a Client connecting
        handleClientConnection(socket, firstMessage);
      }
    } catch (IOException e) {
      System.err.println("Error handling connection: " + e.getMessage());
      try {
        socket.close();
      } catch (IOException ex) {
        System.err.println("Error closing socket: " + ex.getMessage());
      }
    }
  }
  private void handleDstoreConnection(Socket socket, String joinMessage) {
    try {
      // Parse JOIN message to get port
      String[] tokens = joinMessage.split(" ");
      if (tokens.length != 2) {
        System.err.println("Malformed JOIN message: " + joinMessage);
        socket.close();
        return;
      }
      int dstorePort = Integer.parseInt(tokens[1]);

      // Create DstoreInfo object and add to dstores map
      DstoreInfo dstoreInfo = new DstoreInfo(dstorePort, socket);

      if (rebalanceInProgress) {
        // Queue the JOIN operation to be processed after rebalance
        queuedOperations.add(new QueuedOperation(OperationType.JOIN, dstoreInfo, null, null));
        return;
      }

      dstoresLock.writeLock().lock();
      try {
        dstores.put(dstorePort, dstoreInfo);
        System.out.println("Dstore joined on port " + dstorePort);
      } finally {
        dstoresLock.writeLock().unlock();
      }

      // Start a thread to listen for messages from this Dstore
      new Thread(() -> listenForDstoreMessages(dstoreInfo)).start();

      // If we now have enough Dstores and this is a new Dstore, start a rebalance
      if (getDstoreCount() >= r) {
        startRebalance();
      }

    } catch (IOException | NumberFormatException e) {
      System.err.println("Error handling Dstore connection: " + e.getMessage());
      try {
        socket.close();
      } catch (IOException ex) {
        System.err.println("Error closing socket: " + ex.getMessage());
      }
    }
  }

  private void handleDstoreDisconnection(DstoreInfo dstore) {
    dstoresLock.writeLock().lock();
    try {
      int dstorePort = dstore.getPort();
      dstores.remove(dstorePort);
      System.out.println("Dstore disconnected: " + dstorePort);

      // Update replication manager
      Set<String> affectedFiles = replicationManager.handleDstoreDisconnection(dstorePort);
      System.out.println("Dstore " + dstorePort + " disconnected. Affected files: " + affectedFiles);

      // Check if we have enough Dstores to maintain replication factor
      int remainingDstores = getDstoreCount();
      if (remainingDstores < r) {
        // Critical situation: Cannot maintain replication factor
        System.out.println("*************** WARNING ***************");
        System.out.println("SYSTEM OPERATING IN DEGRADED STATE");
        System.out.println("Not enough Dstores remaining (have " + remainingDstores +
            ", need " + r + ") to maintain replication factor");
        System.out.println("Affected files are under-replicated: " + affectedFiles);
        System.out.println("Add more Dstores to restore redundancy");
        System.out.println("***************************************");

        // Mark these files as under-replicated in ReplicationManager
        if (!affectedFiles.isEmpty()) {
          replicationManager.markFilesAsUnderReplicated(affectedFiles);
        }
      } else if (!rebalanceInProgress && !affectedFiles.isEmpty()) {
        // We have enough Dstores, start rebalance to restore replication
        System.out.println("Starting urgent rebalance to restore replication factor for " +
            affectedFiles.size() + " affected files");
        startRebalance();
      } else if (rebalanceInProgress && !affectedFiles.isEmpty()) {
        // Rebalance already in progress
        System.out.println("Cannot start rebalance for affected files immediately - " +
            "rebalance already in progress. Will handle in next scheduled rebalance.");

        // Queue a rebalance request
        queuedOperations.add(new QueuedOperation(OperationType.REBALANCE, null, null, null));
      }
    } finally {
      dstoresLock.writeLock().unlock();
    }

  }

  private void handleClientConnection(Socket socket, String firstMessage) {
    try {
      // Create PrintWriter for sending responses back to client
      PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);

      // If rebalance is in progress, queue the operation
      if (rebalanceInProgress) {
        queuedOperations.add(new QueuedOperation(OperationType.CLIENT_MESSAGE, null, socket, firstMessage));
        return;
      }

      // Process the first message from the client
      processClientMessage(socket, firstMessage, out);

      // Start a thread to listen for further messages from this client
      new Thread(() -> listenForClientMessages(socket)).start();

    } catch (IOException e) {
      System.err.println("Error handling client connection: " + e.getMessage());
      try {
        socket.close();
      } catch (IOException ex) {
        System.err.println("Error closing socket: " + ex.getMessage());
      }
    }
  }

  private void listenForDstoreMessages(DstoreInfo dstore) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(dstore.getSocket().getInputStream()));
      String message;

      while ((message = in.readLine()) != null) {
        processDstoreMessage(dstore, message);
      }

      // If we reach this point, the connection has been closed
      handleDstoreDisconnection(dstore);

    } catch (IOException e) {
      System.err.println("Error reading from Dstore: " + e.getMessage());
      handleDstoreDisconnection(dstore);
    }
  }

  private void listenForClientMessages(Socket clientSocket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()), true);
      String message;

      while ((message = in.readLine()) != null) {
        // If rebalance is in progress, queue the operation
        if (rebalanceInProgress) {
          queuedOperations.add(new QueuedOperation(OperationType.CLIENT_MESSAGE, null, clientSocket, message));
          continue;
        }

        processClientMessage(clientSocket, message, out);
      }

      // If we reach this point, the client has disconnected
      clientSocket.close();

    } catch (IOException e) {
      System.err.println("Error reading from client: " + e.getMessage());
      try {
        clientSocket.close();
      } catch (IOException ex) {
        System.err.println("Error closing client socket: " + ex.getMessage());
      }
    }
  }

  private void processDstoreMessage(DstoreInfo dstore, String message) {
    String[] tokens = message.split(" ");
    String command = tokens[0];

    System.out.println("Received from Dstore " + dstore.getPort() + ": " + message);

    switch (command) {
      case Protocol.STORE_ACK_TOKEN:
        handleStoreAck(dstore, tokens);
        break;
      case Protocol.REMOVE_ACK_TOKEN:
        handleRemoveAck(dstore, tokens);
        break;
      case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN:
        handleFileDoesNotExist(dstore, tokens);
        break;
      case Protocol.LIST_TOKEN:
        handleDstoreList(dstore, tokens);
        break;
      case Protocol.REBALANCE_COMPLETE_TOKEN:
        handleRebalanceComplete(dstore);
        break;
      default:
        System.err.println("Unknown message from Dstore: " + message);
    }
  }

  private void processClientMessage(Socket clientSocket, String message, PrintWriter out) {
    String[] tokens = message.split(" ");
    String command = tokens[0];

    System.out.println("Received from client: " + message);

    // Check if we have enough Dstores
    dstoresLock.readLock().lock();
    boolean enoughDstores = dstores.size() >= r;
    dstoresLock.readLock().unlock();

    if (!enoughDstores) {
      out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return;
    }

    // Process the command
    switch (command) {
      case Protocol.STORE_TOKEN:
        handleStore(clientSocket, tokens, out);
        break;
      case Protocol.LOAD_TOKEN:
        handleLoad(clientSocket, tokens, out);
        break;
      case Protocol.RELOAD_TOKEN:
        handleReload(clientSocket, tokens, out);
        break;
      case Protocol.REMOVE_TOKEN:
        handleRemove(clientSocket, tokens, out);
        break;
      case Protocol.LIST_TOKEN:
        handleList(clientSocket, out);
        break;
      default:
        System.err.println("Unknown message from client: " + message);
    }
  }

  private void handleStoreAck(DstoreInfo dstore, String[] tokens) {
    if (tokens.length != 2) {
      System.err.println("Malformed STORE_ACK message");
      return;
    }

    String filename = tokens[1];
    System.out.println("Received STORE_ACK for " + filename + " from Dstore " + dstore.getPort());

    // Update replication manager and check if we've received all ACKs
    boolean allAcksReceived = replicationManager.handleStoreAck(filename, dstore.getPort());

    if (allAcksReceived) {
      System.out.println("All ACKs received for " + filename + ", marking store complete");

      // Notify the client that the store operation is complete
      Socket clientSocket = replicationManager.getStoreClientSocket(filename);
      if (clientSocket != null && !clientSocket.isClosed()) {
        try {
          PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()), true);
          out.println(Protocol.STORE_COMPLETE_TOKEN);
          System.out.println("Sent STORE_COMPLETE to client for " + filename);
        } catch (IOException e) {
          System.err.println("Error sending STORE_COMPLETE: " + e.getMessage());
        }
      }

      // Complete the store operation in both replication manager and index
      replicationManager.completeStoreOperation(filename);
      index.completeStoreOperation(filename);
    } else {
      System.out.println("Waiting for more ACKs for " + filename);
    }
  }

  private void handleRemoveAck(DstoreInfo dstore, String[] tokens) {
    if (tokens.length != 2) {
      System.err.println("Malformed REMOVE_ACK message");
      return;
    }

    String filename = tokens[1];
    System.out.println("Received REMOVE_ACK for " + filename + " from Dstore " + dstore.getPort());

    // Update replication manager and check if we've received all ACKs
    boolean allAcksReceived = replicationManager.handleRemoveAck(filename, dstore.getPort());

    if (allAcksReceived) {
      System.out.println("All ACKs received for " + filename + ", completing remove operation");

      // Notify the client that the remove operation is complete
      Socket clientSocket = replicationManager.getRemoveClientSocket(filename);
      if (clientSocket != null && !clientSocket.isClosed()) {
        try {
          PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()), true);
          out.println(Protocol.REMOVE_COMPLETE_TOKEN);
          System.out.println("Sent REMOVE_COMPLETE to client for " + filename);
        } catch (IOException e) {
          System.err.println("Error sending REMOVE_COMPLETE: " + e.getMessage());
        }
      }

      // Complete the remove operation in both replication manager and index
      replicationManager.completeRemoveOperation(filename);
      index.completeRemoveOperation(filename);
    } else {
      System.out.println("Waiting for more ACKs for remove of " + filename);
    }
  }

  private void handleFileDoesNotExist(DstoreInfo dstore, String[] tokens) {
    if (tokens.length != 2) {
      System.err.println("Malformed ERROR_FILE_DOES_NOT_EXIST message");
      return;
    }

    String filename = tokens[1];
    System.out.println("Received ERROR_FILE_DOES_NOT_EXIST for " + filename + " from Dstore " + dstore.getPort());

    // Treat as REMOVE_ACK for remove operations
    handleRemoveAck(dstore, new String[]{Protocol.REMOVE_ACK_TOKEN, filename});
  }

  private void handleDstoreList(DstoreInfo dstore, String[] tokens) {
    System.out.println("Received LIST response from Dstore " + dstore.getPort());

    // Extract the list of files from the message
    Set<String> files = new HashSet<>();
    for (int i = 1; i < tokens.length; i++) {
      files.add(tokens[i]);
    }

    // Update the replication manager with this Dstore's files
    replicationManager.updateDstoreFiles(dstore.getPort(), files);

    // If this is part of a rebalance operation, check if we've received all LIST responses
    synchronized (this) {
      dstore.setListReceived(true);

      if (rebalanceInProgress && areAllListsReceived()) {
        calculateAndSendRebalanceCommands();
      }
    }
  }

  private void handleRebalanceComplete(DstoreInfo dstore) {
    System.out.println("Received REBALANCE_COMPLETE from Dstore " + dstore.getPort());

    synchronized (this) {
      dstore.setRebalanceComplete(true);

      if (rebalanceInProgress && areAllRebalancesComplete()) {
        finishRebalance();
      }
    }
  }

  private void handleStore(Socket clientSocket, String[] tokens, PrintWriter out) {
    List<Integer> selectedDstores = selectDstoresForStore();
    if (tokens.length != 3) {
      System.err.println("Malformed STORE message");
      return;
    }

    String filename = tokens[1];
    System.out.println("Selected Dstores for storing " + filename + ": " + selectedDstores);

    int filesize = Integer.parseInt(tokens[2]);

    // Check if file already exists
    if (index.fileExists(filename)) {
      out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
      return;
    }

    // Select Dstores for storage
    if (selectedDstores.isEmpty()) {
      out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return;
    }

    // Add file to index as "store in progress"
    index.addFile(filename, filesize, clientSocket);

    // Initialize store operation in replication manager
    replicationManager.beginStoreOperation(filename, filesize, selectedDstores, clientSocket);

    // Build STORE_TO message
    StringBuilder storeToMessage = new StringBuilder(Protocol.STORE_TO_TOKEN);
    for (int dstorePort : selectedDstores) {
      storeToMessage.append(" ").append(dstorePort);
    }
    out.println(storeToMessage.toString());
  }

  private void handleLoad(Socket clientSocket, String[] tokens, PrintWriter out) {
    if (tokens.length != 2) {
      System.err.println("Malformed LOAD message");
      return;
    }

    String filename = tokens[1];

    // Check if file exists
    if (!index.fileExists(filename) || !index.isStoreComplete(filename)) {
      out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }

    // Get file size and select a Dstore for loading
    int filesize = index.getFileSize(filename);

    // Create a set to track tried Dstores for this client and file
    Set<Integer> triedDstores = new HashSet<>();

    // Select a Dstore that has the file
    int selectedDstore = replicationManager.selectDstoreForLoad(filename, triedDstores);

    if (selectedDstore == -1) {
      // No Dstore has the file
      out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }

    // Update tried Dstores
    triedDstores.add(selectedDstore);

    // Store the set of tried Dstores in the index for this client and file
    index.setTriedDstores(clientSocket, filename, triedDstores);

    // Send LOAD_FROM message to client
    out.println(Protocol.LOAD_FROM_TOKEN + " " + selectedDstore + " " + filesize);
  }

  private void handleReload(Socket clientSocket, String[] tokens, PrintWriter out) {
    if (tokens.length != 2) {
      System.err.println("Malformed RELOAD message");
      return;
    }

    String filename = tokens[1];

    // Check if file exists
    if (!index.fileExists(filename) || !index.isStoreComplete(filename)) {
      out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }

    // Get file size
    int filesize = index.getFileSize(filename);

    // Get the set of tried Dstores for this client and file
    Set<Integer> triedDstores = index.getTriedDstores(clientSocket, filename);

    // Select a different Dstore that has the file
    int selectedDstore = replicationManager.selectDstoreForLoad(filename, triedDstores);

    if (selectedDstore == -1) {
      // No Dstore has the file or all have been tried
      out.println(Protocol.ERROR_LOAD_TOKEN);
      return;
    }

    // Update tried Dstores
    triedDstores.add(selectedDstore);

    // Send LOAD_FROM message to client
    out.println(Protocol.LOAD_FROM_TOKEN + " " + selectedDstore + " " + filesize);
  }

  private void handleRemove(Socket clientSocket, String[] tokens, PrintWriter out) {
    if (tokens.length != 2) {
      System.err.println("Malformed REMOVE message");
      return;
    }

    String filename = tokens[1];

    // Check if file exists
    if (!index.fileExists(filename) || !index.isStoreComplete(filename)) {
      out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }

    // Get the Dstores that have the file
    Set<Integer> dstores = replicationManager.getDstoresForFile(filename);
    if (dstores.isEmpty()) {
      out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }

    // Mark file for removal in index
    index.startRemoveOperation(filename, clientSocket);

    // Begin remove operation in replication manager
    replicationManager.beginRemoveOperation(filename, clientSocket);

    // Send REMOVE message to all Dstores that have the file
    for (int dstorePort : dstores) {
      sendRemoveCommand(dstorePort, filename);
    }
  }

  private void sendRemoveCommand(int dstorePort, String filename) {
    DstoreInfo dstore = getDstore(dstorePort);
    if (dstore != null) {
      try {
        PrintWriter dstoreOut = new PrintWriter(
            new OutputStreamWriter(dstore.getSocket().getOutputStream()), true);
        dstoreOut.println(Protocol.REMOVE_TOKEN + " " + filename);
        System.out.println("Sent REMOVE to Dstore " + dstorePort + " for file " + filename);
      } catch (IOException e) {
        System.err.println("Error sending REMOVE to Dstore: " + e.getMessage());
      }
    }
  }

  private void handleList(Socket clientSocket, PrintWriter out) {
    List<String> files = index.getCompleteFiles();

    StringBuilder listMsg = new StringBuilder(Protocol.LIST_TOKEN);
    for (String file : files) {
      listMsg.append(" ").append(file);
    }

    out.println(listMsg.toString());
  }



  private List<Integer> selectDstoresForStore() {

    // Get all available Dstores
    Set<Integer> availableDstores = getDstorePorts();

    // Let the replication manager select the Dstores
    return replicationManager.selectDstoresForStore(availableDstores);
  }

  private DstoreInfo getDstore(int port) {
    dstoresLock.readLock().lock();
    try {
      return dstores.get(port);
    } finally {
      dstoresLock.readLock().unlock();
    }
  }

  private Set<Integer> getDstorePorts() {
    dstoresLock.readLock().lock();
    try {
      return new HashSet<>(dstores.keySet());
    } finally {
      dstoresLock.readLock().unlock();
    }
  }

  private int getDstoreCount() {
    dstoresLock.readLock().lock();
    try {
      return dstores.size();
    } finally {
      dstoresLock.readLock().unlock();
    }
  }

  private synchronized void startRebalance() {
    System.out.println("Starting rebalance operation with " + getDstoreCount() + " active Dstores");
    if (rebalanceInProgress || getDstoreCount() < r) {
      return;
    }

    System.out.println("Starting rebalance operation");
    rebalanceInProgress = true;

    // Reset the list and rebalance flags for all Dstores
    dstoresLock.readLock().lock();
    try {
      for (DstoreInfo dstore : dstores.values()) {
        dstore.setListReceived(false);
        dstore.setRebalanceComplete(false);
      }
    } finally {
      dstoresLock.readLock().unlock();
    }

    // Send LIST to all Dstores
    dstoresLock.readLock().lock();
    try {
      for (DstoreInfo dstore : dstores.values()) {
        try {
          PrintWriter dstoreOut = new PrintWriter(
              new OutputStreamWriter(dstore.getSocket().getOutputStream()), true);
          dstoreOut.println(Protocol.LIST_TOKEN);
          System.out.println("Sent LIST to Dstore " + dstore.getPort() + " for rebalance");
        } catch (IOException e) {
          System.err.println("Error sending LIST to Dstore: " + e.getMessage());
        }
      }
    } finally {
      dstoresLock.readLock().unlock();
    }

    // The rest of the rebalance process continues in handleDstoreList and calculateAndSendRebalanceCommands
  }

  private boolean areAllListsReceived() {
    dstoresLock.readLock().lock();
    try {
      for (DstoreInfo dstore : dstores.values()) {
        if (!dstore.isListReceived()) {
          return false;
        }
      }
      return true;
    } finally {
      dstoresLock.readLock().unlock();
    }
  }

  private void calculateAndSendRebalanceCommands() {
    System.out.println("All LIST responses received, calculating rebalance commands");

    // Get active Dstores
    List<Integer> activeDstores = new ArrayList<>();
    Map<Integer, Set<String>> dstoreFiles = new HashMap<>();

    dstoresLock.readLock().lock();
    try {
      for (Map.Entry<Integer, DstoreInfo> entry : dstores.entrySet()) {
        int dstorePort = entry.getKey();
        activeDstores.add(dstorePort);
        dstoreFiles.put(dstorePort, replicationManager.getFilesOnDstore(dstorePort));

      }
    } finally {
      dstoresLock.readLock().unlock();
    }

    // Calculate rebalance operations
    Map<Integer, ReplicationManager.RebalanceOperation> rebalanceOps =
        replicationManager.calculateRebalance(activeDstores, dstoreFiles);

    // Send rebalance commands to Dstores
    dstoresLock.readLock().lock();
    try {
      for (Map.Entry<Integer, DstoreInfo> entry : dstores.entrySet()) {
        int dstorePort = entry.getKey();
        DstoreInfo dstore = entry.getValue();
        ReplicationManager.RebalanceOperation op = rebalanceOps.get(dstorePort);

        if (op == null) {
          continue;
        }

        // Construct REBALANCE message
        StringBuilder message = new StringBuilder(Protocol.REBALANCE_TOKEN);

        // Add files to send
        message.append(" ").append(op.filesToSend.size());
        for (Map.Entry<String, List<Integer>> fileEntry : op.filesToSend.entrySet()) {
          String filename = fileEntry.getKey();
          List<Integer> destinations = fileEntry.getValue();

          message.append(" ").append(filename).append(" ").append(destinations.size());
          for (int destPort : destinations) {
            message.append(" ").append(destPort);
          }
        }

        // Add files to remove
        message.append(" ").append(op.filesToRemove.size());
        for (String filename : op.filesToRemove) {
          message.append(" ").append(filename);
        }

        // Send the message
        try {
          PrintWriter dstoreOut = new PrintWriter(
              new OutputStreamWriter(dstore.getSocket().getOutputStream()), true);
          dstoreOut.println(message.toString());
          System.out.println("Sent REBALANCE to Dstore " + dstorePort);
          System.out.println("Sending rebalance command to Dstore " + dstorePort +
              ": " + op.filesToSend.size() + " files to send, " +
              op.filesToRemove.size() + " files to remove");
        } catch (IOException e) {
          System.err.println("Error sending REBALANCE to Dstore: " + e.getMessage());
        }
      }
    } finally {
      dstoresLock.readLock().unlock();
    }

    // If no Dstores are available, finish the rebalance
    if (activeDstores.isEmpty()) {
      finishRebalance();
    }

    // Otherwise, wait for REBALANCE_COMPLETE messages from all Dstores
  }

  private boolean areAllRebalancesComplete() {
    dstoresLock.readLock().lock();
    try {
      for (DstoreInfo dstore : dstores.values()) {
        if (!dstore.isRebalanceComplete()) {
          return false;
        }
      }
      return true;
    } finally {
      dstoresLock.readLock().unlock();
    }
  }

  private void finishRebalance() {
    System.out.println("Rebalance operation completed. Processing " +
        queuedOperations.size() + " queued operations");
    rebalanceInProgress = false;

    // NEW CODE: Refresh file tracking by requesting LIST from all Dstores
    dstoresLock.readLock().lock();
    try {
      for (DstoreInfo dstore : dstores.values()) {
        try {
          PrintWriter dstoreOut = new PrintWriter(
              new OutputStreamWriter(dstore.getSocket().getOutputStream()), true);
          dstoreOut.println(Protocol.LIST_TOKEN);
          System.out.println("Sent LIST to Dstore " + dstore.getPort() + " to refresh tracking");
        } catch (IOException e) {
          System.err.println("Error sending LIST to Dstore: " + e.getMessage());
        }
      }
    } finally {
      dstoresLock.readLock().unlock();
    }

    // Process queued operations
    processQueuedOperations();
  }

  private void processQueuedOperations() {
    QueuedOperation op;
    while ((op = queuedOperations.poll()) != null) {
      switch (op.type) {
        case JOIN:
          dstoresLock.writeLock().lock();
          try {
            DstoreInfo dstoreInfo = op.dstoreInfo;
            dstores.put(dstoreInfo.getPort(), dstoreInfo);
            System.out.println("Queued Dstore joined on port " + dstoreInfo.getPort());

            // Start a thread to listen for messages from this Dstore
            new Thread(() -> listenForDstoreMessages(dstoreInfo)).start();

            // Send LIST to initialize tracking
            try {
              PrintWriter dstoreOut = new PrintWriter(
                  new OutputStreamWriter(dstoreInfo.getSocket().getOutputStream()), true);
              dstoreOut.println(Protocol.LIST_TOKEN);
              System.out.println("Sent LIST to queued Dstore " + dstoreInfo.getPort() + " to initialize tracking");
            } catch (IOException e) {
              System.err.println("Error sending LIST to queued Dstore: " + e.getMessage());
            }
          } finally {
            dstoresLock.writeLock().unlock();
          }
          break;
        case CLIENT_MESSAGE:
          try {
            PrintWriter out = new PrintWriter(new OutputStreamWriter(op.clientSocket.getOutputStream()), true);
            processClientMessage(op.clientSocket, op.message, out);
          } catch (IOException e) {
            System.err.println("Error processing queued client message: " + e.getMessage());
          }
          break;
        case REBALANCE:
          // Process queued rebalance request
          if (!rebalanceInProgress && getDstoreCount() >= r) {
            System.out.println("Processing queued rebalance after previous rebalance completion");
            startRebalance();
          }
          break;
      }
    }
  }

  // Inner class to represent a Dstore
  private class DstoreInfo {
    private final int port;
    private final Socket socket;
    private boolean listReceived;
    private boolean rebalanceComplete;

    public DstoreInfo(int port, Socket socket) {
      this.port = port;
      this.socket = socket;
      this.listReceived = false;
      this.rebalanceComplete = false;
    }

    public int getPort() {
      return port;
    }

    public Socket getSocket() {
      return socket;
    }

    public boolean isListReceived() {
      return listReceived;
    }

    public void setListReceived(boolean listReceived) {
      this.listReceived = listReceived;
    }

    public boolean isRebalanceComplete() {
      return rebalanceComplete;
    }

    public void setRebalanceComplete(boolean rebalanceComplete) {
      this.rebalanceComplete = rebalanceComplete;
    }
  }

  private enum OperationType {
    JOIN,
    CLIENT_MESSAGE,
    REBALANCE
  }

  private class QueuedOperation {
    private final OperationType type;
    private final DstoreInfo dstoreInfo;
    private final Socket clientSocket;
    private final String message;

    public QueuedOperation(OperationType type, DstoreInfo dstoreInfo, Socket clientSocket, String message) {
      this.type = type;
      this.dstoreInfo = dstoreInfo;
      this.clientSocket = clientSocket;
      this.message = message;
    }
  }
}