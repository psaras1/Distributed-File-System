# Distributed File System

This project implements a distributed file storage system with replication capabilities. The system consists of a Controller and multiple Data Stores (Dstores), supporting file operations such as store, load, list, and remove with built-in fault tolerance.

## Architecture

The system follows a Controller-Dstore architecture:

- **Controller**: Orchestrates all operations and maintains an index of file locations
- **Dstores**: Store the actual file data with replication across multiple Dstores
- **Client**: Provided as a compiled JAR to interact with the system

Files are replicated across R different Dstores to ensure fault tolerance. If Dstores fail or new ones join, the system performs rebalance operations to maintain the replication factor.

## Running the System

### Prerequisites

- Java 21 JDK
- client.jar (provided)

### Setup Folders

```bash
mkdir downloads to_store
echo "This is test file 1" > to_store/testfile1.txt
echo "This is test file 2" > to_store/testfile2.txt
```

### Starting the Components

1. Start the Controller:
   ```bash
   java Controller <cport> <R> <timeout> <rebalance_period>
   ```
   - `cport`: Controller port number
   - `R`: Replication factor (number of copies for each file)
   - `timeout`: Timeout in milliseconds
   - `rebalance_period`: Time between rebalance operations in seconds

2. Start at least R Dstores:
   ```bash
   java Dstore <port> <cport> <timeout> <file_folder>
   ```
   - `port`: Port for this Dstore
   - `cport`: Controller port
   - `timeout`: Timeout in milliseconds
   - `file_folder`: Folder to store files

3. Run the Client:
   ```bash
   java -cp client.jar:. ClientMain <cport> <timeout>
   ```
   - `cport`: Controller port
   - `timeout`: Timeout in milliseconds

Example:
```bash
# Start Controller with replication factor 3
java Controller 12345 3 1000 60

# Start 3 Dstores
java Dstore 10001 12345 1000 dstore1_folder
java Dstore 10002 12345 1000 dstore2_folder
java Dstore 10003 12345 1000 dstore3_folder

# Run the client
java -cp client.jar:. ClientMain 12345 1000
```

## Client Workflow

The client interacts with the system through the `ClientMain.java` file, which uses:

- `to_store/`: Files in this folder are uploaded to the distributed system
- `downloads/`: Downloaded files are stored here

The client performs the following operations:
1. Lists all files in the system
2. Uploads files from the `to_store/` folder
3. Lists files again to verify storage
4. Downloads the files to the `downloads/` folder
5. Removes files from the system

## Testing

### Basic Tests
- Store, load, and remove files using the client
- Check that files appear in the correct Dstore folders
- Verify content integrity between uploaded and downloaded files

### Fault Tolerance Tests
1. **Dstore Failure Test**:
   - Start Controller and at least R+1 Dstores
   - Store some files
   - Kill one Dstore
   - Verify the system still functions if at least R Dstores remain

2. **Rebalance Test**:
   - Start Controller and R Dstores
   - Store some files
   - Add a new Dstore
   - Wait for the rebalance period
   - Verify files are redistributed evenly

## Protocol Details

The system uses TCP connections with a text-based protocol:

### Store Operation
- Client → Controller: `STORE filename filesize`
- Controller → Client: `STORE_TO port1 port2 ... portR`
- Client connects to each Dstore and sends the file

### Load Operation
- Client → Controller: `LOAD filename`
- Controller → Client: `LOAD_FROM port filesize`
- Client connects to the Dstore to download the file

### List Operation
- Client → Controller: `LIST`
- Controller → Client: `LIST file1 file2 ...`

### Remove Operation
- Client → Controller: `REMOVE filename`
- Controller removes the file from all Dstores
- Controller → Client: `REMOVE_COMPLETE`

## Error Handling

The system handles several error conditions:
- `ERROR_NOT_ENOUGH_DSTORES`: When fewer than R Dstores are available
- `ERROR_FILE_ALREADY_EXISTS`: When trying to store a file that already exists
- `ERROR_FILE_DOES_NOT_EXIST`: When trying to load or remove a non-existent file
- `ERROR_LOAD`: When a file cannot be loaded from any available Dstore

## Rebalance Operation

The rebalance operation ensures:
1. Files are replicated R times
2. Files are distributed evenly across Dstores

The Controller initiates rebalance:
- Periodically based on `rebalance_period`
- When a new Dstore joins the system

During rebalance, client operations are queued and processed after rebalance completes.
