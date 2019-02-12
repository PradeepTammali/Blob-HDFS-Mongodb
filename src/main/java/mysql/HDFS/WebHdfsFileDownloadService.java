package mysql.HDFS;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;

public class WebHdfsFileDownloadService {
	public InputStream downloadFile(String urlTo,String fileName) throws ParseException, IOException {
		//System.out.println("Entered into WebHDFSFileDownloadService.downloadFile() controller method");
		HttpURLConnection connection = null;
		InputStream inputStream = null;
	//	OutputStream outputStream = null;

	
		//String boundary = "*****" + Long.toString(System.currentTimeMillis()) + "*****";
	
 
 
		try {
			//FileInputStream fileInputStream = new FileInputStream(inputStream2);
 
			URL url = new URL(urlTo);
			connection = (HttpURLConnection) url.openConnection();
 
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
 
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Connection", "Keep-Alive");
			connection.setRequestProperty("User-Agent", "Mozilla/5.0");
			connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=");
 
			inputStream = connection.getInputStream();
			//result = this.convertStreamToString(inputStream);
			byte[] bytesInputstream = IOUtils.toByteArray(inputStream);
			InputStream myInputStream = new ByteArrayInputStream(bytesInputstream); 
			
			/*outputStream =
                    new FileOutputStream(new File("D:\\Downloads\\download"+fileName));

		int read = 0;
		byte[] bytes = new byte[1024];

		while ((read = inputStream.read(bytes)) != -1) {
			outputStream.write(bytes, 0, read);
		}
		outputStream.close();*/
			
			inputStream.close();
			
			
			//System.out.println("Exiting from WebHDFSFileDownloadService.downloadFile() controller method");
			return myInputStream;
			//return result;
		} catch (Exception e) {
			System.out.println("Multipart Form Upload Error");
			e.printStackTrace();
			System.out.println("Exiting from WebHDFSFileDownloadService.downloadFile() controller method error");
			//return "error";
			return null;
		}
	}
 
	/*private String convertStreamToString(InputStream is) {
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
	}*/
}