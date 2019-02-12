package mysql.HDFS;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.ContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

@Path("/Service")
public class MysqlService {  

	//user authentication 
	@GET
	@Path("Login")
	@Produces(javax.ws.rs.core.MediaType.TEXT_PLAIN)
public Response validateUser(@QueryParam("username") String userName,@QueryParam("password") String password)
{
		String response = null;
	try
	  {
	    String myUrl = "jdbc:mysql://localhost:3306/db";
	    Connection conn = DriverManager.getConnection(myUrl, "root", "");
		// our SQL SELECT query. 
		// if you only need a few columns, specify them by name instead of using "*
		String res = "select username, password from login where username = '"+userName+"'";
		// create the java statement
		 PreparedStatement statement = conn.prepareStatement(res);
		 
		// execute the query, and get a java result set
		ResultSet rs = statement.executeQuery(res);
		//System.out.println("rs------"+rs);

		//JsonObject jsonResponse = new JsonObject();	
		
			if(!rs.next())
			{
				System.out.println("\nRequested username does not exist");
			}
			else if(userName.equals(rs.getString("username")) && password.equals(rs.getString("password"))){
				response = "valid";
			}
			else
			{
				response = "invalid";
			}
			conn.close();
	  }
	  catch (Exception e)
	  {
	    System.err.println("Got an exception!");
	    System.err.println(e.getMessage());
	    e.printStackTrace();
	  }	
	return Response.ok("Login").entity(response).header("Access-Control-Allow-Origin", "*").build();
}	
		
//loading data into mysql 
	 @POST
	 @Path("loadIntoSql")
	 @Produces(javax.ws.rs.core.MediaType.TEXT_PLAIN)
	 @Consumes(javax.ws.rs.core.MediaType.MULTIPART_FORM_DATA)
public Response loadIntoSql(@FormDataParam("workOrderId") int id,@FormDataParam("description") String description ,@FormDataParam("fileName") List<FormDataBodyPart> fileNames)
{
		 
	try
	  {
		for(FormDataBodyPart files : fileNames){
			String fileName = files.getValueAs(String.class);
		
		//fileName=fileName.substring(fileName.lastIndexOf("/") + 1);
		//fileName=fileName.substring(fileName.lastIndexOf("\\") + 1);
		String path ="/user/tcs/"+fileName;
	    // create a mysql database connection
	    //String myDriver = "org.apache.derby.jdbc.EmbeddedDriver";
	    String myUrl = "jdbc:mysql://localhost:3306/db";
	    //Class.forName(myDriver);
	    Connection conn = DriverManager.getConnection(myUrl, "root", "");
	  
	    String sql = "INSERT INTO sample (id, description, path) values (?, ?,?)";
	    PreparedStatement statement = conn.prepareStatement(sql);
	    statement.setInt(1, id);
	    statement.setString(2, description);
	    statement.setString(3, path);
	    
	    int row = statement.executeUpdate();
	    if (row > 0) {
	        System.out.println("A row is inserted");
	    } 
	    conn.close();
		}
	  }
	  catch (Exception e)
	  {
	    System.err.println("Got an exception!");
	    System.err.println(e.getMessage());
	    e.printStackTrace();
	  }	
	return Response.ok("loadIntoSql").entity("File saved").header("Access-Control-Allow-Origin", "*").build(); 
}

	 //retrieve the data from mysql and passing to UI
	 @GET
	 @Path("retriveFromSql")
	 @Produces(MediaType.TEXT_PLAIN)
public Response retriveFromSql(@QueryParam("workOrderId") int id)
{
		// StringBuilder sb = new StringBuilder();
		 JsonArray data = new JsonArray();
	try
	  {
	    String myUrl = "jdbc:mysql://localhost:3306/db";
	    Connection conn = DriverManager.getConnection(myUrl, "root", "");
		// our SQL SELECT query. 
		// if you only need a few columns, specify them by name instead of using "*
		String res = "select  id, description, path from sample where id = '"+id +"'";
		// create the java statement
		 PreparedStatement statement = conn.prepareStatement(res);
		 
		// execute the query, and get a java result set
		ResultSet rs = statement.executeQuery(res);
		//System.out.println("rs------"+rs);

		//JsonObject jsonResponse = new JsonObject();	
		
		if(!rs.next())
		{
			System.out.println("\nRequested work order id does not exist");
		}
		else{
	      do
	      {
	    	  JsonObject innerobj = new JsonObject();
	    	  innerobj.add("id", new JsonPrimitive(rs.getString("id")));
	    	  innerobj.add("description", new JsonPrimitive(rs.getString("description")));
	    	  innerobj.add("path", new JsonPrimitive(rs.getString("path")));
	    	  data.add(innerobj);
	    	  
	      }while (rs.next());
	      //jsonResponse.add("retrieve",data);
		}
	System.out.println("JSONOBJ-----"+data.toString());
	    conn.close();
	  }
	  catch (Exception e)
	  {
	    System.err.println("Got an exception!");
	    System.err.println(e.getMessage());
	    e.printStackTrace();
	  }	
	return Response.ok("retriveFromSql").entity(data.toString()).header("Access-Control-Allow-Origin", "*").build();
}
	 	 
	 //retrieve file from HDFS and download to a local path  D:\Downloads\
	 @GET
	 @Path("retrivefile")
	 @Produces(MediaType.MULTIPART_FORM_DATA)
public Response openFile(@QueryParam("fileName") String fileName) throws IOException {
		 
		 WebHdfsDemoControllerdownload wb =new WebHdfsDemoControllerdownload();
		 fileName = fileName.substring(fileName.lastIndexOf("\\")+1);
		 fileName=fileName.substring(fileName.lastIndexOf("/") + 1);
		 //String output = wb.openFile(fileName);
		 //output = null;
		 
		 InputStream output = wb.openFile(fileName);
		// InputStream output1 = new FileInputStream("D:\\Downloads\\download"+fileName);
		 System.out.println(fileName);
		// Response.WriteFile(Server.MapPath("~/Images/"+fileName+""));
		 //.header("Content-type", "text/html; charset=utf-8")
		 //.header("Content-Encoding","none")
		 return Response.ok("retrivefile").entity(output).header("Access-Control-Allow-Origin", "*").header("content-disposition", "attachment; filename="+fileName).header("content-type", "application/octet-stream").build();
	 }
	 
	 
	 //Put the data into HDFS from the UI
	 @POST
	 @Path("uploadfile")
	 @Produces(MediaType.TEXT_PLAIN)
	 @Consumes(MediaType.MULTIPART_FORM_DATA)
public Response createFile(FormDataMultiPart  files) throws IOException, ParseException {
		
		 
		 List<FormDataBodyPart> fields = files.getFields("file");  
		 for(FormDataBodyPart field : fields){
			 ContentDisposition meta = field.getContentDisposition();
			 String fileName = meta.getFileName();
			 byte[] bytesInputstream = IOUtils.toByteArray(field.getValueAs(InputStream.class));
			 InputStream myInputStream = new ByteArrayInputStream(bytesInputstream); 

		 /*BASE64Decoder decoder = new BASE64Decoder();
		 byte[] decodedBytes = decoder.decodeBuffer(file);
		 InputStream is = new ByteArrayInputStream(decodedBytes);*/
		 
		 //http://stackoverflow.com/questions/23979842/convert-base64-string-to-image
		 //http://stackoverflow.com/questions/1802123/can-we-convert-a-byte-array-into-an-inputstream-in-java
		 //http://stackoverflow.com/questions/19980307/stream-decoding-of-base64-data
	  
			 System.out.println("file name:"+fileName);
			 WebhdfsDemoControllerupload wb =new WebhdfsDemoControllerupload();
			 wb.createFile(fileName,myInputStream);
	     }
		 /*Response.ok().header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
		 Response.ok().header("Access-Control-Allow-Credentials", "true");
		 Response.ok().header("Access-Control-Allow-Origin", "*");
		 Response.ok().header("Access-Control-Allow-Headers", "Content-Type,X-Requested-With");
		 Response.ok().header("Access-Control-Max-Age", "60");*/
		 return Response.ok("uploadfile").entity("Done").header("Access-Control-Allow-Origin", "*").build();
	 }
	 
/*public static void main(String[] args)
{
	MysqlService obj =new MysqlService();
  obj.loadIntoSql(103, "5-9-17 103-pushed ","103file");
  //obj.retriveFromSql(102);
  //obj.loadIntoHDFS(); //this method does not work yet.....still in progress
}*/

}