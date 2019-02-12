package mysql.HDFS;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;


public class HDFSPutFileService {

	String userName = "hdfs";
	
	public String createFile(String fileName,String hdfsPath, InputStream file) throws ParseException, MalformedURLException, IOException {
			
		HttpURLConnection connection = null;
		DataOutputStream outputStream = null;
		InputStream inputStream = null;
		String mkdirUrl = "http://localhost:50075/webhdfs/v1" + hdfsPath + fileName + "?user.name=" + userName + "&op=CREATE"+"&namenoderpcaddress=localhost:8020&overwrite=true";
		
		String result = "" ;
		 
		int bytesRead, bytesAvailable, bufferSize;
		byte[] buffer;
		int maxBufferSize = 1 * 1024 * 1024;
		try {
 
			URL url = new URL(mkdirUrl);
			connection = (HttpURLConnection) url.openConnection();
 
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
 
			connection.setRequestMethod("PUT");
			connection.setRequestProperty("Connection", "Keep-Alive");
			connection.setRequestProperty("User-Agent", "Mozilla/5.0");
			connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" );
 
			outputStream = new DataOutputStream(connection.getOutputStream());
 
			bytesAvailable = file.available();
			bufferSize = Math.min(bytesAvailable, maxBufferSize);
			buffer = new byte[bufferSize];
 
			bytesRead = file.read(buffer, 0, bufferSize);
			while (bytesRead> 0) {
				outputStream.write(buffer, 0, bufferSize);
				bytesAvailable = file.available();
				bufferSize = Math.min(bytesAvailable, maxBufferSize);
				bytesRead = file.read(buffer, 0, bufferSize);
			}	
			
			inputStream = connection.getInputStream();
			file.close();
			inputStream.close();
			outputStream.flush();
			outputStream.close();
			result = "File saved";
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("\nExiting from HDFSPutFileService error");
			return "Error";
		}
	}
	
	public static void main(String args[]) throws MalformedURLException, ParseException, IOException{
		HDFSPutFileService putObj = new HDFSPutFileService();
		HDFSGetFileService getObj = new HDFSGetFileService();
		//putObj.createFile("social_friends.csv", "/user/cloudera/", getObj.getFromHdfs("social_friends.csv", "/user/tcs/"));
		System.out.println(putObj.createFile("social_friends.csv", "/user/ravi/", getObj.getFromHdfs("social_friends.csv", "/user/tcs/")));
	}
}
