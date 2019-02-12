package mongoDB;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class MongoDBService {
	
	public String insertDocument(int id,String description,List<String> fileName,List<String> fileId){
		try{		
	         // To connect to mongodb server
	         @SuppressWarnings("resource")
			 MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
				
	         // Now connect to your databases
	         @SuppressWarnings("deprecation")
			 DB db = mongoClient.getDB( "DataInjestion" );
	         System.out.println("Connect to database successfully");
	         
//	         boolean auth = db.authenticate(myUserName, myPassword);
//	         System.out.println("Authentication: "+auth);
				
	         DBCollection coll = db.getCollection("data");
	         System.out.println("Collection mycol selected successfully"); 
	         
	         BasicDBObject file = new BasicDBObject();
	         for(int i=0;i<fileName.size();i++){
	        	 file.put(fileId.get(i), fileName.get(i));
	         }
	         
	         
	         BasicDBObject document = new BasicDBObject();
	         document.put("id", id);
	         document.put("description", description);
	         document.put("files", file);
	        
	         coll.insert(document); 
	         System.out.println("Document inserted successfully");
	         return "Saved";
	         
	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	         return "Error";
	      }
	}
	
	public String insertFile(InputStream file){
		String fileId = "";
		try{
			
	         // To connect to mongodb server
	         @SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
				
	         // Now connect to your databases
	         @SuppressWarnings("deprecation")
			DB db = mongoClient.getDB( "DataInjestion" );
	         System.out.println("Connect to database successfully");	         
	         
	         GridFS gfs = new GridFS(db, "file");
	         GridFSInputFile gfsFile = gfs.createFile(file);
	         gfsFile.save();
	         fileId = gfsFile.getId().toString();
	        // System.out.println("file stored successfully-----------"+gfsFile.getId());

	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }
		return fileId;
	}
	
	public String retrieveDocument(int id){
		
		JsonArray data = new JsonArray();
		try{
			
	         // To connect to mongodb server
	         @SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
				
	         // Now connect to your databases
	         @SuppressWarnings("deprecation")
			 DB db = mongoClient.getDB( "DataInjestion" );
	         System.out.println("Connect to database successfully");
	         
//	         boolean auth = db.authenticate(myUserName, myPassword);
//	         System.out.println("Authentication: "+auth);
				
	         DBCollection coll = db.getCollection("data");
	         System.out.println("Collection mycol selected successfully");
	         
	         BasicDBObject whereQuery = new BasicDBObject();
	         whereQuery.put("id", id);
	         
	         DBCursor cursor = coll.find(whereQuery);
	         while (cursor.hasNext()) {  	        	
	        	 BasicDBObject obj = (BasicDBObject) cursor.next();
	        	 JsonObject jsonobj = new JsonObject();

	        	 jsonobj.addProperty("id", obj.getString("id"));
	        	 jsonobj.addProperty("description", obj.getString("description"));
	        	 jsonobj.addProperty("files", obj.getString("files"));
	        	 data.add(jsonobj); 
	         }
	         
	         System.out.println("JSONOBJ-----"+data.toString());
	         return data.toString();
	         
	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	         return "Error";
	      }
	}

	public InputStream retrieveFile(String fileId){
		
		try{			
	         // To connect to mongodb server
	         @SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
				
	         // Now connect to your databases
	         @SuppressWarnings("deprecation")
			DB db = mongoClient.getDB( "DataInjestion" );
	         System.out.println("Connect to database successfully");
	         
	         GridFS gfs = new GridFS(db, "file");
	         GridFSDBFile file = gfs.findOne(new ObjectId(fileId));

	         InputStream is = file.getInputStream();
	         byte[] bytesInputstream = IOUtils.toByteArray(is);
			 InputStream myInputStream = new ByteArrayInputStream(bytesInputstream);
	         
	         System.out.println(file);
	         System.out.println("file retrieved successfully");
	         return myInputStream;
	         
	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	         return null;
	      }
	}
	
		
	/*	public static void main( String args[] ) throws FileNotFoundException {
			int id=23;
			//String description = "something_here";
			//String fileName = "sampleMongoDBFile1";
			
			InputStream file1 = new FileInputStream("D:\\Downloads\\login.html");
			InputStream file2 = new FileInputStream("C:\\Users\\1115334\\Desktop\\sample.txt");
			InputStream file3 = new FileInputStream("C:\\Users\\1115334\\Desktop\\script.txt");
			
			List<InputStream> list = new ArrayList<InputStream>(); 
			list.add(file1);
			list.add(file2);
			list.add(file3);
			
			List<String> listFileId = new ArrayList<String>();
		MongoDBService dbObject = new MongoDBService();
		for(int i=0;i<list.size();i++){
			listFileId.add(dbObject.insertFile(list.get(i)));
		}
		
		 List<String> listFileName = new ArrayList<String>();
		 listFileName.add("file1");
		 listFileName.add("file2");
		 listFileName.add("file3");
		 
		 for(int i=0;i<list.size();i++){
				System.out.println(listFileId.get(i));
			}
		 
			//dbObject.insertDocument(id,description,listFileName,listFileId);
			dbObject.retrieveDocument(id);
					//dbObject.retrieveFile("591ed327c3480d1a2884ae89");
		}*/
}
