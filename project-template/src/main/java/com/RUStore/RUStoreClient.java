package com.RUStore;

/* any necessary Java packages here */
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;


public class RUStoreClient {

	/* any necessary class members here */
	private String host;
	private int port;
	private Socket sock;
	private DataInputStream fromServer;
	private DataOutputStream toServer;

	/**
	 * RUStoreClient Constructor, initializes default values
	 * for class members
	 *
	 * @param host	host url
	 * @param port	port number
	 */
	public RUStoreClient(String host, int port) {

		// Implement here
		this.host = host;
		this.port = port;

	}

	/**
	 * Opens a socket and establish a connection to the object store server
	 * running on a given host and port.
	 *
	 * @return		n/a, however throw an exception if any issues occur
	 * @throws IOException 
	 */
	public void connect() throws IOException { //get rid of "throws IOException" if causing issues

		// Implement here
		sock = new Socket(this.host, this.port); //do one of the exceptions it recommends
		fromServer = new DataInputStream(sock.getInputStream());
		toServer = new DataOutputStream(sock.getOutputStream());
		

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT be 
	 * overwritten
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param data	byte array representing arbitrary data object
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int put(String key, byte[] data) throws IOException{
		// Check if the socket is not null and connected
	    if (sock == null || !sock.isConnected()) {
	    	sock.close();
	        throw new IOException("Not connected to the server");
	    }
		
      
		
		// Serialize key
	    byte[] keyBytes = key.getBytes();
	    
	    toServer.writeUTF("PUT"); //have server receive this
	    
	    //sends  serialized key
	    toServer.writeInt(keyBytes.length);
	    toServer.write(keyBytes);
	    
	    //read response from server
	    int response = fromServer.readInt(); //server will send back 0 for good and 1 for key alr exists
	    //System.out.println("put server response: " + response);
	    
		if (response == 0) {
		   // Send the data length
			toServer.writeInt(data.length);
			//System.out.println("Initial data length: " + data.length);
			// Send the data itself
			toServer.write(data);
	            
			//after data is sent, get message saying the data is sent
			int dataDeliveredResponse = fromServer.readInt();
			if (dataDeliveredResponse == 0) {
				return 0; //success
			}
			else {
				sock.close();
				throw new IOException("No response from server. Issue with data being sent");
			}
		}
		else if (response == 1) {
			return 1; //key already exists
		}
		else {
			sock.close();
			throw new IOException("Server error: " + response);
		}
	    
		
		// Implement here
		
		//return 0;	
		

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT 
	 * be overwritten.
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param file_path	path of file data to transfer
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int put(String key, String file_path) throws IOException {

		// Implement here
		if (sock == null || !sock.isConnected()) {
	    	sock.close();
	        throw new IOException("Not connected to the server");
	    }
		
		//edge case for file
		File file = new File(file_path);
		if (!file.exists() || !file.isFile()) {
	        throw new IOException("File does not exist or is not a regular file");
	    }

		FileInputStream fileInputStream = new FileInputStream(file);
	    byte[] fileData = new byte[(int) file.length()];

	    try {
	        // Read file data into the byte array
	        fileInputStream.read(fileData);
	    } finally {
	        fileInputStream.close();
	    }

	    // Call the first put method with the key and file data
	    return put(key, fileData);
	


	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server.
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		object data as a byte array, null if key doesn't exist.
	 *        		Throw an exception if any other issues occur.
	 * @throws IOException 
	 */
	public byte[] get(String key) throws IOException {

		// Implement here
		//send GET command
		//send the key to server
		//server will send back the byte array for the key
		//return the byte array
		
		// Check if the socket is not null and connected
	    if (sock == null || !sock.isConnected()) {
	    	sock.close();
	        throw new IOException("Not connected to the server");
	    }
	    
//	    fromServer = new DataInputStream(sock.getInputStream());
//		toServer = new DataOutputStream(sock.getOutputStream());
		
	    byte[] keyBytes = key.getBytes();
	    //byte[] keyBytes = key.getBytes();
	    toServer.writeUTF("GET");
	    toServer.writeInt(keyBytes.length);
	    toServer.write(keyBytes);
	    //if server sends back 0, then key exists and continue. if server sends back 1, key does not exist and return null
	    int response = fromServer.readInt();
	    //System.out.println("response from server: " + response);
	    //System.out.println("test");
	    if (response == 0) {
	    	int dataLength = fromServer.readInt();
	    	byte[] dataChars = new byte[dataLength];
			fromServer.readFully(dataChars);
			
			
			return dataChars;
	    }
	    else if (response == 1){
	    	return null;
	    }
	    else {
	    	throw new IOException("Unknown response from server- issue with key");
	    }
		
		
		//return null;

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server and places it in a file. 
	 * 
	 * @param key	key associated with the object
	 * @param	file_path	output file path
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int get(String key, String file_path) throws IOException {

		// Implement here
		if (file_path == null) {
			throw new IOException("File path does not exist");
		}
		
		FileOutputStream outputFile = new FileOutputStream(file_path);
		
		 byte[] arr = get(key);
		 if (arr == null) {
			 outputFile.close();
			 return 1;
		 }
		 else {
			 outputFile.write(arr);
			 outputFile.close();
			 return 0;
		 }
		
		//return -1;

	}

	/**
	 * Removes data object associated with a given key 
	 * from the object store server. Note: No need to download the data object, 
	 * simply invoke the object store server to remove object on server side
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int remove(String key) throws IOException {

		// Implement here
		if (sock == null || !sock.isConnected()) {
		    sock.close();
		    throw new IOException("Not connected to the server");
		}
		
		 byte[] keyBytes = key.getBytes("UTF-8");
		 toServer.writeUTF("REMOVE");
		 toServer.writeInt(keyBytes.length);
		 toServer.write(keyBytes);
		
		 int response = fromServer.read();
		 if (response == 0) {
			 return 0;
		 }
		 else if (response == 1){
			 return 1;
		 }
		 else {
			 throw new IOException("Invalid server response");
		 }
		 
		 
		 //return -1;

	}

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return		List of keys as string array, null if there are no keys.
	 *        		Throw an exception if any other issues occur.
	 * @throws IOException 
	 */
	public String[] list() throws IOException {

		// Implement here
		if (sock == null || !sock.isConnected()) {
		    sock.close();
		    throw new IOException("Not connected to the server");
		}
		toServer.writeUTF("LIST");
		int keySetLength = fromServer.readInt();  //receive length of string list
		if (keySetLength == 0) {
			return null;
		}
		List<String> receivedStrings = new ArrayList<String>();
		for (int i = 0; i < keySetLength; i++) {
			String str = fromServer.readUTF();
			receivedStrings.add(str);
		}
		
		String[] arr = receivedStrings.toArray(new String[0]); //POTENTIAL ISSUES WITH THIS- CHECK
		
		return arr;

	}

	/**
	 * Signals to server to close connection before closes 
	 * the client socket.
	 * 
	 * @return		n/a, however throw an exception if any issues occur
	 * @throws IOException 
	 */
	public void disconnect() throws IOException {

		// Implement here
		if (sock == null || !sock.isConnected()) {
	    	sock.close();
	        throw new IOException("Not connected to the server");
	    }
		
		toServer.writeUTF("CLOSE");
		
		sock.close();

	}

}
