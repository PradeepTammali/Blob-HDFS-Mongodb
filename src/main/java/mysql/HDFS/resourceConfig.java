package mysql.HDFS;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;


    /**
     * Registers the components to be used by the JAX-RS application  
     *
     */
//@ApplicationPath("/Service/uploadfile")
public class resourceConfig extends ResourceConfig {

     /**
        * Register JAX-RS application components.
        */  

        public resourceConfig(){
            register(MysqlService.class);  
            register(mongoDB.MongoDBController.class);
            register(App.class);
            register(MultiPartFeature.class);
        }

}