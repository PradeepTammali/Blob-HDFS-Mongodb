package mysql.HDFS;


import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.text.ParseException;
//import org.glassfish.jersey.media.multipart.FormDataParam;

//@Path("/work")
public class WebhdfsDemoControllerupload  {

	
	String userName = "hdfs";
//	@Path("/createFile")
//	@POST
//	@Consumes(MediaType.MULTIPART_FORM_DATA)
//	@Produces(MediaType.APPLICATION_JSON)
	
	public void createFile(String fileName,  InputStream file) throws ParseException {
		//System.out.println("Entered into WebhdfsDemoController.createFile() controller method");
		//String output = null;
		try {
			String baseUrl = "http://localhost:50075/webhdfs/v1/user/tcs/";
			//String baseUrl = "http://<datanode_host>:<datanode_ip>/webhdfs/v1/";
 
			String mkdirUrl = fileName + "?user.name=" + userName + "&op=CREATE&namenoderpcaddress=localhost:8020&overwrite=true";
 
			mkdirUrl = baseUrl + mkdirUrl;
 
			//System.out.println("createDirectory() hitting the url is: " + mkdirUrl);
 
			WebHDFSFileUploadService service = new WebHDFSFileUploadService();
			
			//File sourceFile = new File(inputStream.toString());//Write a core java logic to convert inputstream object to File object
			
			try {
				service.uploadFile(mkdirUrl, file);
			} catch (org.apache.commons.cli.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
 
			//System.out.println("createFile output: " + output);			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
 
		//System.out.println("Exiting from WebhdfsDemoController.createFile() controller method");
	}
			
	/*public static void main(String args[]) throws ParseException
	{
		try {
		//File file = new File("sample.txt");
			File file = new File("C:\\Users\\Public\\Pictures\\Sample Pictures\\Chrysanthemum.jpg");
		InputStream ob = new FileInputStream(file);
		WebhdfsDemoControllerupload obj = new WebhdfsDemoControllerupload();
		obj.createFile("pradeep.jpg", ob);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}*/
}


