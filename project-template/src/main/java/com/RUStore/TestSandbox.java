package com.RUStore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

/**
 * This TestSandbox is meant for you to implement and extend to 
 * test your object store as you slowly implement both the client and server.
 * 
 * If you need more information on how an RUStorageClient is used
 * take a look at the RUStoreClient.java source as well as 
 * TestSample.java which includes sample usages of the client.
 */
public class TestSandbox{
	

	public static void main(String[] args) {
		String stringKey;
		String stringValue;
		int ret;
		byte[] retBytes;
		String outString;
		RUStoreClient client;
		String fileKey;
		String inputPath;
		String outputPath;
		File fileIn;
		File fileOut;
		byte[] fileInBytes;
		byte[] fileOutBytes;  

		// Create a new RUStoreClient
		String host = "localhost";
		Integer port = 1234;
		client = new RUStoreClient(host, port);

		// Open a connection to a remote service
		System.out.println("Connecting to object server at " + host + ":" + port + "...");
		try {
			client.connect();
			System.out.println("Established connection to server.");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to connect to server.");
		}
		
		// Store String
		stringKey = "str_1";
		stringValue = "Hello World";
//
//			// PUT String
			try {
				System.out.println("Putting string \"" + stringValue + "\" with key \"" + stringKey + "\"");
				ret = client.put(stringKey, stringValue.getBytes());
				if(ret == 0) {
					System.out.println("Successfully put string and key!");
				}else {
					System.out.println("Failed to put string \"" + stringValue + "\" with key \"" + stringKey + "\". Key already exists. (INCORRECT RETURN)");
				}
			} catch (Exception e1) {
				e1.printStackTrace();
				System.out.println("Failed to put string \"" + stringValue + "\" with key \"" + stringKey + "\". Exception occured.");
			}
			
			String stringKey2 = "str_2";
			String stringValue2 = "Hi Computer";
	//
//				// PUT String
				try {
					System.out.println("Putting string \"" + stringValue2 + "\" with key \"" + stringKey2 + "\"");
					ret = client.put(stringKey2, stringValue2.getBytes());
					if(ret == 0) {
						System.out.println("Successfully put string and key!");
					}else {
						System.out.println("Failed to put string \"" + stringValue2 + "\" with key \"" + stringKey2 + "\". Key already exists. (INCORRECT RETURN)");
					}
				} catch (Exception e1) {
					e1.printStackTrace();
					System.out.println("Failed to put string \"" + stringValue2 + "\" with key \"" + stringKey2 + "\". Exception occured.");
				}
				
			//list data storage
				try {
					System.out.println("Listing keys in data storage");
					String[] arr = client.list();
					for (String s: arr) {
						System.out.println(s + " ");
					}
				} catch(Exception e1){
					e1.printStackTrace();
					System.out.println("Failed to put string \"" + stringValue + "\" with key \"" + stringKey + "\". Exception occured.");
				}
			
			
			
			//remove key from datathing
			try {
				System.out.println("Removing \"" + stringKey);
				ret = client.remove(stringKey);
				if (ret == 0) {
					System.out.println("Successfully removed string and key!");
				}
				else if (ret == 1) {
					System.out.println("Key does not exist in data storage! (WRONG OUTPUT)");
				}
				else {
					System.out.println("ERROR. WRONG REMOVE OUTPUT.");
				}
				
			} catch(Exception e1){
				e1.printStackTrace();
				System.out.println("Failed to put string \"" + stringValue + "\" with key \"" + stringKey + "\". Exception occured.");
			}
			
			
		
//		//GET
//		try {
//			System.out.println("Getting object with key \"" + stringKey + "\"");
//			byte[] retBytes = client.get(stringKey);
//			String outString = new String(retBytes);
//			System.out.println("What GET returns: " + outString);
//			if(retBytes != null) {
//				if(stringValue.equals(outString)) {
//					System.out.println("Successfully got string: " + outString);
//				}else {
//					System.out.println("Failed to get back string, got garbage data.");
//				}
//			}else {
//				System.out.println("Failed getting object with key \"" + stringKey + "\". Key doesn't exist. (INCORRECT RETURN)");
//			}
//		} catch (Exception e1) {
//			e1.printStackTrace();
//			System.out.println("Failed getting object with key \"" + stringKey + "\" Exception occured.");
//		}
		
		
		
		fileKey = "chapter1.txt";
		inputPath = "./inputfiles/lofi.mp3";
		outputPath = "./outputfiles/lofi_.mp3";

		// PUT File
//		try {
//			System.out.println("Putting file \"" + inputPath + "\" with key \"" + fileKey + "\"");
//			ret = client.put(fileKey, inputPath);
//			if(ret == 0) {
//				System.out.println("Successfully put file!");
//			}else {
//				System.out.println("Failed to put file \"" + inputPath + "\" with key \"" + fileKey + "\". Key already exists. (INCORRECT RETURN)");
//			}
//		} catch (Exception e1) {
//			e1.printStackTrace();
//			System.out.println("Failed to put file \"" + inputPath + "\" with key \"" + fileKey + "\". Exception occured.");
//		} 
//
//		// GET File
//		try {
//			System.out.println("Getting object with key \"" + fileKey + "\"");
//			ret = client.get(fileKey, outputPath);
//			if(ret == 0) {
//				fileIn = new File(inputPath);
//				fileOut = new File(outputPath);
//				if(fileOut.exists()) {
//					fileInBytes = Files.readAllBytes(fileIn.toPath());
//					fileOutBytes = Files.readAllBytes(fileOut.toPath());
//					if(Arrays.equals(fileInBytes, fileOutBytes)) {
//						System.out.println("File contents are equal! Successfully Retrieved File");
//					}else {
//						System.out.println("File contents are not equal! Got garbage data. (BAD FILE DOWNLOAD)");
//					}
//					System.out.println("Deleting downloaded file.");
//					Files.delete(fileOut.toPath());
//				}else {
//					System.out.println("No file downloaded. (BAD FILE DOWNLOAD)");
//				}
//			}else {
//				System.out.println("Failed getting object with key \"" + stringKey + "\". Key doesn't exist. (INCORRECT RETURN)");
//			}
//		} catch (IOException e1) {
//			e1.printStackTrace();
//			System.out.println("Failed getting object with key \"" + stringKey + "\" Exception occured.");
//		}

		// Disconnect
				System.out.println("Attempting to disconnect...");
				try {
					client.disconnect();
					System.out.println("Sucessfully disconnected.");
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Failed to disconnect.");
				}

	}

}
