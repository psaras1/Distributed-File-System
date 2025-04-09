import java.io.File;

public class ClientMain1 {
  public static void main(String[] args) {
    if (args.length < 3) {
      System.out.println("Usage: java -cp client.jar:. ClientMain cport timeout command [args...]");
      System.exit(1);
    }

    int cport = Integer.parseInt(args[0]);
    int timeout = Integer.parseInt(args[1]);
    String command = args[2];

    try {
      Client client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
      client.connect();

      switch (command) {
        case "store":
          if (args.length != 4) {
            System.out.println("Usage: store filename");
            break;
          }
          client.store(new File(args[3]));
          break;

        case "load":
          if (args.length != 5) {
            System.out.println("Usage: load filename destination");
            break;
          }
          client.load(args[3], new File(args[4]));
          break;

        case "remove":
          if (args.length != 4) {
            System.out.println("Usage: remove filename");
            break;
          }
          client.remove(args[3]);
          break;

        case "list":
          String[] files = client.list();
          System.out.println("Files in storage:");
          if (files.length == 0) {
            System.out.println("  (no files)");
          } else {
            for (String file : files) {
              System.out.println("  " + file);
            }
          }
          break;

        default:
          System.out.println("Unknown command: " + command);
      }

      client.disconnect();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}