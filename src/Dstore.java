import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class Dstore {

  // Dstore parameters
  private final int port;
  private final int cport;
  private final int timeout;
  private final String fileFolder;

  // Socket for connection to Controller
  private Socket controllerSocket;
  private PrintWriter controllerOut;
  private BufferedReader controllerIn;

  // Server socket to accept client connections
  private ServerSocket serverSocket;

  // Thread pool for handling client connections
  private ExecutorService clientThreadPool;

  // Thread for handling controller messages
  private Thread controllerThread;

  // Flag to indicate if the Dstore is running
  private volatile boolean running;

  // Keep track of rebalance operations
  private ConcurrentHashMap<String, List<Integer>> filesToSend;
  private List<String> filesToRemove;
  private final Object rebalanceLock = new Object();

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java Dstore port cport timeout file_folder");
      System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    int cport = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    String fileFolder = args[3];

    new Dstore(port, cport, timeout, fileFolder).start();
  }

  public Dstore(int port, int cport, int timeout, String fileFolder) {
    this.port = port;
    this.cport = cport;
    this.timeout = timeout;
    this.fileFolder = fileFolder;
    this.clientThreadPool = Executors.newCachedThreadPool();
    this.filesToSend = new ConcurrentHashMap<>();
    this.filesToRemove = new CopyOnWriteArrayList<>();
    this.running = true;
  }

  public void start() {
    try {
      // Create file folder if it doesn't exist, or clean it if it does
      File folder = new File(fileFolder);
      if (folder.exists()) {
        // Clean the folder
        for (File file : folder.listFiles()) {
          file.delete();
        }
      } else {
        folder.mkdir();
      }

      // Connect to Controller
      connectToController();

      // Start server socket to listen for client connections
      serverSocket = new ServerSocket(port);
      System.out.println("Dstore started on port " + port);

      // Start a thread to listen for messages from the Controller
      controllerThread = new Thread(this::handleControllerMessages);
      controllerThread.start();

      // Main loop to accept client connections
      while (running) {
        try {
          Socket clientSocket = serverSocket.accept();
          clientThreadPool.submit(() -> handleClientConnection(clientSocket));
        } catch (IOException e) {
          if (running) {
            System.err.println("Error accepting client connection: " + e.getMessage());
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Dstore error: " + e.getMessage());
      e.printStackTrace();
    } finally {
      shutdown();
    }
  }

  private void connectToController() throws IOException {
    controllerSocket = new Socket("localhost", cport);
    // Don't set timeout on the entire socket - this causes disconnections during quiet periods
    // controllerSocket.setSoTimeout(timeout);
    controllerOut = new PrintWriter(new OutputStreamWriter(controllerSocket.getOutputStream()), true);
    controllerIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

    // Send JOIN message to Controller
    controllerOut.println(Protocol.JOIN_TOKEN + " " + port);
    System.out.println("Sent JOIN message to Controller");
  }

  private void handleControllerMessages() {
    try {
      String message;
      while (running && (message = controllerIn.readLine()) != null) {
        System.out.println("Received from Controller: " + message);
        processControllerMessage(message);
      }
    } catch (IOException e) {
      if (running) {
        System.err.println("Lost connection to Controller: " + e.getMessage());
        running = false;
      }
    }
  }

  private void processControllerMessage(String message) {
    String[] tokens = message.split(" ");
    String command = tokens[0];

    switch (command) {
      case Protocol.REMOVE_TOKEN:
        handleRemove(tokens);
        break;
      case Protocol.LIST_TOKEN:
        handleList();
        break;
      case Protocol.REBALANCE_TOKEN:
        handleRebalance(tokens);
        break;
      default:
        System.err.println("Unknown message from Controller: " + message);
    }
  }

  private void handleClientConnection(Socket clientSocket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()), true);
      String message = in.readLine();

      if (message != null) {
        System.out.println("Received from client: " + message);
        String[] tokens = message.split(" ");
        String command = tokens[0];

        switch (command) {
          case Protocol.STORE_TOKEN:
            handleStore(clientSocket, in, out, tokens);
            break;
          case Protocol.LOAD_DATA_TOKEN:
            handleLoad(clientSocket, out, tokens);
            break;
          case Protocol.REBALANCE_STORE_TOKEN:
            handleRebalanceStore(clientSocket, in, out, tokens);
            break;
          default:
            System.err.println("Unknown message from client: " + message);
        }
      }

      clientSocket.close();
    } catch (IOException e) {
      System.err.println("Error handling client connection: " + e.getMessage());
      try {
        clientSocket.close();
      } catch (IOException ex) {
        System.err.println("Error closing client socket: " + ex.getMessage());
      }
    }
  }

  private void handleStore(Socket clientSocket, BufferedReader in, PrintWriter out, String[] tokens) throws IOException {
    if (tokens.length != 3) {
      System.err.println("Malformed STORE message");
      return;
    }

    String filename = tokens[1];
    int filesize = Integer.parseInt(tokens[2]);

    // Send ACK to client
    out.println(Protocol.ACK_TOKEN);

    // Receive file content
    File file = new File(fileFolder, filename);
    FileOutputStream fileOut = new FileOutputStream(file);
    byte[] buffer = new byte[1024];
    int bytesRead;
    int totalBytesRead = 0;

    InputStream clientIn = clientSocket.getInputStream();

    // Read the entire file
    while (totalBytesRead < filesize) {
      int bytesToRead = Math.min(buffer.length, filesize - totalBytesRead);
      bytesRead = clientIn.read(buffer, 0, bytesToRead);

      if (bytesRead == -1) {
        break; // End of stream
      }

      fileOut.write(buffer, 0, bytesRead);
      totalBytesRead += bytesRead;
    }

    fileOut.close();

    // Send STORE_ACK to Controller
    controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + filename);
    System.out.println("Sent STORE_ACK to Controller for file: " + filename);
  }

  private void handleLoad(Socket clientSocket, PrintWriter out, String[] tokens) throws IOException {
    if (tokens.length != 2) {
      System.err.println("Malformed LOAD_DATA message");
      return;
    }

    String filename = tokens[1];
    File file = new File(fileFolder, filename);

    if (!file.exists()) {
      System.err.println("File not found: " + filename);
      clientSocket.close();
      return;
    }

    // Send file content to client
    FileInputStream fileIn = new FileInputStream(file);
    OutputStream clientOut = clientSocket.getOutputStream();
    byte[] buffer = new byte[1024];
    int bytesRead;

    while ((bytesRead = fileIn.read(buffer)) != -1) {
      clientOut.write(buffer, 0, bytesRead);
    }

    fileIn.close();
    System.out.println("Sent file to client: " + filename);
  }

  private void handleRemove(String[] tokens) {
    if (tokens.length != 2) {
      System.err.println("Malformed REMOVE message");
      return;
    }

    String filename = tokens[1];
    File file = new File(fileFolder, filename);

    if (file.exists()) {
      if (file.delete()) {
        System.out.println("Deleted file: " + filename);
        controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
      } else {
        System.err.println("Failed to delete file: " + filename);
      }
    } else {
      System.err.println("File not found: " + filename);
      controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
    }
  }

  private void handleList() {
    File folder = new File(fileFolder);
    File[] files = folder.listFiles();
    StringBuilder fileList = new StringBuilder(Protocol.LIST_TOKEN);

    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          fileList.append(" ").append(file.getName());
        }
      }
    }

    // Send the response to the Controller
    String response = fileList.toString();
    controllerOut.println(response);
    System.out.println("Sent file list to Controller: " + response);
  }

  private void handleRebalance(String[] tokens) {

    synchronized (rebalanceLock) {
      // Parse the rebalance command
      try {
        int index = 1;
        int numFilesToSend = Integer.parseInt(tokens[index++]);
        int numFilesToRemove = Integer.parseInt(tokens[index + numFilesToSend * (2 + 1)]);
        System.out.println("Received rebalance command: " + numFilesToSend +
            " files to send, " + numFilesToRemove + " files to remove");

        // Parse files to send
        filesToSend.clear();

        for (int i = 0; i < numFilesToSend; i++) {
          String filename = tokens[index++];
          int numDstores = Integer.parseInt(tokens[index++]);
          List<Integer> destinations = new ArrayList<>();

          for (int j = 0; j < numDstores; j++) {
            destinations.add(Integer.parseInt(tokens[index++]));
          }

          filesToSend.put(filename, destinations);
        }

        // Parse files to remove
        filesToRemove = new ArrayList<>();

        for (int i = 0; i < numFilesToRemove; i++) {
          filesToRemove.add(tokens[index++]);
        }

        // Start the rebalance process
        new Thread(this::performRebalance).start();

      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        System.err.println("Error parsing REBALANCE message: " + e.getMessage());
        controllerOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
      }

    }
  }

  private void performRebalance() {
    synchronized (rebalanceLock) {
      try {
        // First send files to other Dstores
        for (Map.Entry<String, List<Integer>> entry : filesToSend.entrySet()) {
          String filename = entry.getKey();
          List<Integer> destinations = entry.getValue();
          File file = new File(fileFolder, filename);

          if (!file.exists()) {
            System.err.println("File not found for rebalance: " + filename);
            continue;
          }

          long filesize = file.length();

          for (int dstorePort : destinations) {
            try {
              // Connect to the destination Dstore
              Socket dstoreSocket = new Socket("localhost", dstorePort);
              PrintWriter dstoreOut = new PrintWriter(new OutputStreamWriter(dstoreSocket.getOutputStream()), true);
              BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));

              // Send REBALANCE_STORE message
              dstoreOut.println(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + filesize);

              // Wait for ACK
              String response = dstoreIn.readLine();
              if (response != null && response.equals(Protocol.ACK_TOKEN)) {
                // Send file content
                FileInputStream fileIn = new FileInputStream(file);
                OutputStream out = dstoreSocket.getOutputStream();
                byte[] buffer = new byte[1024];
                int bytesRead;

                while ((bytesRead = fileIn.read(buffer)) != -1) {
                  out.write(buffer, 0, bytesRead);
                }

                fileIn.close();
                System.out.println("Sent file to Dstore " + dstorePort + ": " + filename);
              } else {
                System.err.println("Did not receive ACK from Dstore " + dstorePort);
              }

              dstoreSocket.close();
            } catch (IOException e) {
              System.err.println("Error sending file to Dstore " + dstorePort + ": " + e.getMessage());
            }
          }
        }

        // Then remove files
        for (String filename : filesToRemove) {
          File file = new File(fileFolder, filename);

          if (file.exists()) {
            if (file.delete()) {
              System.out.println("Deleted file during rebalance: " + filename);
            } else {
              System.err.println("Failed to delete file during rebalance: " + filename);
            }
          }
        }

        // Notify Controller that rebalance is complete
        controllerOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
        System.out.println("Sent REBALANCE_COMPLETE to Controller");

      } catch (Exception e) {
        System.err.println("Error during rebalance: " + e.getMessage());
        controllerOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
      }
    }
  }

  private void handleRebalanceStore(Socket clientSocket, BufferedReader in, PrintWriter out, String[] tokens) throws IOException {
    if (tokens.length != 3) {
      System.err.println("Malformed REBALANCE_STORE message");
      return;
    }

    String filename = tokens[1];
    int filesize = Integer.parseInt(tokens[2]);

    // Send ACK to the sending Dstore
    out.println(Protocol.ACK_TOKEN);

    // Receive file content
    File file = new File(fileFolder, filename);
    FileOutputStream fileOut = new FileOutputStream(file);
    byte[] buffer = new byte[1024];
    int bytesRead;
    int totalBytesRead = 0;

    InputStream clientIn = clientSocket.getInputStream();

    // Read the entire file
    while (totalBytesRead < filesize) {
      int bytesToRead = Math.min(buffer.length, filesize - totalBytesRead);
      bytesRead = clientIn.read(buffer, 0, bytesToRead);

      if (bytesRead == -1) {
        break; // End of stream
      }

      fileOut.write(buffer, 0, bytesRead);
      totalBytesRead += bytesRead;
    }

    fileOut.close();
    System.out.println("Received file from rebalance: " + filename);
  }

  private void shutdown() {
    running = false;

    try {
      if (serverSocket != null && !serverSocket.isClosed()) {
        serverSocket.close();
      }

      if (controllerSocket != null && !controllerSocket.isClosed()) {
        controllerSocket.close();
      }

      if (clientThreadPool != null) {
        clientThreadPool.shutdown();
        try {
          if (!clientThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
            clientThreadPool.shutdownNow();
          }
        } catch (InterruptedException e) {
          clientThreadPool.shutdownNow();
        }
      }

      if (controllerThread != null && controllerThread.isAlive()) {
        controllerThread.interrupt();
      }
    } catch (IOException e) {
      System.err.println("Error during shutdown: " + e.getMessage());
    }
  }
}