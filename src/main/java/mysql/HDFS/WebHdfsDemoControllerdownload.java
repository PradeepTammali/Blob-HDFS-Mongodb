package mysql.HDFS;


import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.apache.commons.cli.ParseException;


@Path("/WebhdfsService")
public class WebHdfsDemoControllerdownload {
	String userName = "hdfs";
	 /*@GET
	 @Path("retrivefile")
	 @Produces(MediaType.TEXT_PLAIN)
	// @Produces(MediaType.MULTIPART_FORM_DATA)
*/	 
	public InputStream openFile(@QueryParam("fileName") String fileName) throws IOException {
			//System.out.println("Entered into WebhdfsDemoController.createFile() controller method");
			InputStream output = null;
			//fileName=fileName.substring(fileName.lastIndexOf("/") + 1);
			//fileName=fileName.substring(fileName.lastIndexOf("\\") + 1);
			//System.out.println("fileName"+fileName);
			String baseUrl = "http://localhost:50075/webhdfs/v1/user/tcs/";
			//String baseUrl = "http://<datanode_host>:<datanode_ip>/webhdfs/v1/";
 
			String mkdirUrl = fileName + "?user.name=" + userName + "&op=OPEN"+"&namenoderpcaddress=localhost:8020&overwrite=true";
 
			mkdirUrl = baseUrl + mkdirUrl;
 
			//System.out.println("createDirectory() hitting the url is: " + mkdirUrl);
			
			WebHdfsFileDownloadService service = new WebHdfsFileDownloadService();
			
			//File sourceFile = new File(inputStream.toString());//Write a core java logic to convert inputstream object to File object
			try {
				output = service.downloadFile(mkdirUrl,fileName);
			} catch (ParseException e) {
				e.printStackTrace();
			}
 
			//System.out.println(output);
	//		System.out.println("Exiting from WebhdfsDemoController.createFile() controller method");
		return output;
			//return Response.ok("Test1").entity(output).header("Access-Control-Allow-Origin", "*").build();
	 }
				
		/*public static void main(String args[])
		{
			try {
			//File file = new File("sample.txt");
				//File file = new File("D://Pradeep//urls.txt");
			//FileInputStream ob = new FileInputStream(file);
			WebHdfsDemoControllerdownload obj = new WebHdfsDemoControllerdownload();
		
				System.out.println(obj.openFile("/user/tcs/another1.txt"));
			}
			 catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}*/
	
}
