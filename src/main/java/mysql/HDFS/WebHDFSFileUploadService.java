package mysql.HDFS;


import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.cli.ParseException;

public class WebHDFSFileUploadService {
	public String uploadFile(String urlTo, InputStream file) throws ParseException, IOException {
		//System.out.println("Entered into WebHDFSFileUploadService.uploadFile() controller method");
		HttpURLConnection connection = null;
		DataOutputStream outputStream = null;
		InputStream inputStream = null;
 
		//String twoHyphens = "--";
		//String boundary = "*****" + Long.toString(System.currentTimeMillis()) + "*****";
		//String lineEnd = "\r\n";
		
 
		String result = "" ;
 
		int bytesRead, bytesAvailable, bufferSize;
		byte[] buffer;
		int maxBufferSize = 1 * 1024 * 1024;
 
		try {
			//FileInputStream fileInputStream = new FileInputStream(inputStream2);
 
			URL url = new URL(urlTo);
			connection = (HttpURLConnection) url.openConnection();
 
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
 
			connection.setRequestMethod("PUT");
			connection.setRequestProperty("Connection", "Keep-Alive");
			connection.setRequestProperty("User-Agent", "Mozilla/5.0");
			connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" );
 
			outputStream = new DataOutputStream(connection.getOutputStream());
//			outputStream.writeBytes(twoHyphens + boundary + lineEnd);
//			outputStream.writeBytes("Content-Type: multipart/form-data" + lineEnd);
//			outputStream.writeBytes(lineEnd);
// 
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
 
//			outputStream.writeBytes(lineEnd);
// 
//			outputStream.writeBytes(twoHyphens + boundary + twoHyphens + lineEnd);
			
			
			
			inputStream = connection.getInputStream();
			result = this.convertStreamToString(inputStream);
			//System.out.println("result------"+result);
			file.close();
			inputStream.close();
			outputStream.flush();
			outputStream.close();
 
			//System.out.println("Exiting from WebHDFSFileUploadService.uploadFile() controller method");
			return result;
		} catch (Exception e) {
			System.out.println("Multipart Form Upload Error");
			e.printStackTrace();
			System.out.println("Exiting from WebHDFSFileUploadService.uploadFile() controller method error");
			return "error";
		}
	}
 
	private String convertStreamToString(InputStream is) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();
 
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		//System.out.println(sb.toString());
		return sb.toString();
	}
}