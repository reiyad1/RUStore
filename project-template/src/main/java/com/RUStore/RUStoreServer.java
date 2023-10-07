package com.RUStore;

/* any necessary Java packages here */
import java.io.*;
import java.net.*;
import java.util.HashMap;

public class RUStoreServer {

	/* any necessary class members here */
	private static DataInputStream fromClient;
	private static DataOutputStream toClient;
	private static HashMap<String, byte[]> dataStorage;

	/* any necessary helper methods here */
	
	/**
	 * Handles the put request on server side.
	 * @param fromClient		DataInputStream object for reading from the client
	 * @param toClient			DataOutputStream object for writing to the client
	 * @param dataStorage		Hashmap data structure to store keys and object stores
	 * @throws IOException
	 */ 
	private static void handlePutRequest(DataInputStream fromClient, DataOutputStream toClient, HashMap<String, byte[]> dataStorage) throws IOException {
		
		int zero = 0;
		int one = 1;
		
		int keyLength = fromClient.readInt();
		byte[] keyChars = new byte[keyLength];
		fromClient.readFully(keyChars);
		String key = new String(keyChars);
		
		
		if (dataStorage != null) {
			
			if (!dataStorage.containsKey(key)) {
				toClient.writeInt(zero); //key is not in dataStorage- let client know to send over key and byte array
				
			}
			else {
				toClient.writeInt(one); //key is in dataStorage
				return;
			}
		}
		else {
			toClient.writeInt(zero);
		}

		    
		//Read byte array
		int dataLength = fromClient.readInt();
		byte[] dataChars = new byte[dataLength];
		fromClient.readFully(dataChars);
		//String data = new String(dataChars);
		toClient.writeInt(zero);
		
		
		dataStorage.put(key, dataChars);
		
	}
	/**
	 * Handles the get request on server side.
	 * @param fromClient		DataInputStream object for reading from the client
	 * @param toClient			DataOutputStream object for writing to the client
	 * @param dataStorage		Hashmap data structure to store keys and object stores
	 * @throws IOException
	 */
	private static void handleGetRequest(DataInputStream fromClient, DataOutputStream toClient, HashMap<String, byte[]> dataStorage) throws IOException {
		
		int keyLength = fromClient.readInt();
		byte[] keyChars = new byte[keyLength];
		fromClient.readFully(keyChars);
		String key = new String(keyChars);
		
		if (dataStorage != null) {
			if (dataStorage.containsKey(key)) {
				
				toClient.writeInt(0);
				
				byte[] dataToSend = dataStorage.get(key);

				toClient.writeInt(dataToSend.length);
				toClient.write(dataToSend);
				//System.out.println("Sent data back to client");
			}
			else {
				System.out.println("key not in data storage");
				toClient.writeInt(1);
			}
		}
		else {
			System.out.println("no data in data storage");
		}
	}
	
	
	/**
	 * Handles the remove request on server side.
	 * @param fromClient		DataInputStream object for reading from the client
	 * @param toClient			DataOutputStream object for writing to the client
	 * @param dataStorage		Hashmap data structure to store keys and object stores
	 * @throws IOException
	 */
	private static void handleRemoveRequest(DataInputStream fromClient, DataOutputStream toClient, HashMap<String, byte[]> dataStorage) throws IOException {
		
		int keyLength = fromClient.readInt();
		byte[] keyChars = new byte[keyLength];
		fromClient.readFully(keyChars);
		String key = new String(keyChars);
		if (dataStorage != null) {
			if (dataStorage.containsKey(key)) {
				dataStorage.remove(key);
				toClient.writeInt(0);
			}
			else {
				toClient.writeInt(1);
			}
		}
		else {
			System.out.println("no data in data storage");
		}
	}
	
	/**
	 * Handles the list request on server side.
	 * @param fromClient		DataInputStream object for reading from the client
	 * @param toClient			DataOutputStream object for writing to the client
	 * @param dataStorage		Hashmap data structure to store keys and object stores
	 * @throws IOException
	 */
	private static void handleListRequest(DataInputStream fromClient, DataOutputStream toClient, HashMap<String, byte[]> dataStorage) throws IOException {
		int keySetLength = dataStorage.keySet().size(); //send length of string list
		toClient.writeInt(keySetLength);
		if (keySetLength > 0) {
			for (String key : dataStorage.keySet()) {
				toClient.writeUTF(key);
			}
		}
	}

	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 * @throws IOException 
	 */
	public static void main(String args[]) throws IOException{

		// Check if at least one argument that is potentially a port number
		if(args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}
		
		// Try and parse port # from argument
		int port = Integer.parseInt(args[0]);

		// Implement here //
		//CONNECTING TO CLIENT
		ServerSocket svc = new ServerSocket(port, 5);
		Socket conn = svc.accept(); //get a connection from client
		
		if (conn == null || !conn.isConnected()) {
			svc.close();
	        throw new IOException("Not connected to the client");
	    }
		

		dataStorage = new HashMap<String, byte[]>();
		fromClient = new DataInputStream(conn.getInputStream());
		toClient = new DataOutputStream(conn.getOutputStream());

		//put in while loop potentially
		while(true) {
			String command = fromClient.readUTF();
			
			if (command != null && command.equals("PUT")) {
	            // Handle the PUT request
	            handlePutRequest(fromClient, toClient, dataStorage);
						   
	        }
			else if (command != null && command.equals("GET")) {
				//Handle the GET request
				handleGetRequest(fromClient, toClient, dataStorage);
				
				
			}
			else if (command.equals("REMOVE")) {
				//Handle the REMOVE request
				handleRemoveRequest(fromClient, toClient, dataStorage);
				
			}
			else if (command != null && command.equals("LIST")) {
				//Handle the LIST request
				handleListRequest(fromClient, toClient, dataStorage);
				
			}
			else if (command != null && command.equals("CLOSE")) {
				//close server socket
				svc.close();
				conn.close();
				//toClient.writeUTF("CLOSED");
				break;
			}
			
			
			else {
				System.out.println("Not printing right command: " + command);
				break;
			}

		}

	}

}
