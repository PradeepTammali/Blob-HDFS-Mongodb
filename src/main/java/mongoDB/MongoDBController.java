package mongoDB;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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

@Path("/MongoDBService")
public class MongoDBController {

	 @POST
	 @Path("loadIntoMongo")
	 @Produces(MediaType.TEXT_PLAIN)
	 @Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response loadIntoMongoDb(@FormDataParam("workOrderId") int id,@FormDataParam("description") String description,FormDataMultiPart  files) throws IOException{
		
		 MongoDBService dbObject = new MongoDBService();
		 List<String>  listFileName = new ArrayList<String>();
		 List<String> listFileId = new ArrayList<String>();
			
		 List<FormDataBodyPart> fields = files.getFields("file");
		 for(FormDataBodyPart field : fields){
			 ContentDisposition meta = field.getContentDisposition();
			 String fileName = meta.getFileName();
			 byte[] bytesInputstream = IOUtils.toByteArray(field.getValueAs(InputStream.class));
			 InputStream myInputStream = new ByteArrayInputStream(bytesInputstream); 
			 listFileName.add(fileName);
			 listFileId.add(dbObject.insertFile(myInputStream));
		 }
 
		 String response = dbObject.insertDocument(id, description, listFileName, listFileId);
		 return Response.ok("MongoDBInsert").entity(response).header("Access-Control-Allow-Origin", "*").build();
	}
	 
	 @GET
	 @Path("retrieveFromMongo")
	 @Produces(MediaType.TEXT_PLAIN)
	public Response retrieveFromMongoDb(@QueryParam("workOrderId") int id){
		 
		 MongoDBService mobj = new MongoDBService();
		 String data = mobj.retrieveDocument(id);	 
		 return Response.ok("MongoDBRetrieve").entity(data).header("Access-Control-Allow-Origin", "*").build();
	 }
	 
	@GET
	@Path("retriveFileFromMongo")
	@Produces(MediaType.MULTIPART_FORM_DATA)
		public Response retrieveFileFromMongoDb(@QueryParam("fileName") String fileName,@QueryParam("fileId") String fileId) throws IOException {
		
		MongoDBService mobj = new MongoDBService();
		fileName = fileName.substring(fileName.lastIndexOf("\\")+1);
		fileName=fileName.substring(fileName.lastIndexOf("/") + 1);
			
		InputStream output = mobj.retrieveFile(fileId);
		System.out.println(fileName);	
		return Response.ok("MongoDBFileRetrieve").entity(output).header("Access-Control-Allow-Origin", "*").header("content-disposition", "attachment; filename="+fileName).header("content-type", "application/octet-stream").build();
	}

}
