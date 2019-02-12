package mysql.HDFS;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletProperties;



public class App 
{
    public static void main(String[] args ) throws Exception
    {
/*        System.out.println( "Hello World!" );
        ResourceConfig config = new ResourceConfig();
        config.packages("jettyjerseytutorial");
        ServletHolder servlet = new ServletHolder(new ServletContainer(config));


       Server server = new Server(2222);
        ServletContextHandler context = new ServletContextHandler(server, "/*");
        context.addServlet(servlet, "/*");
        
        */
    	//Map<String, String> map = new HashMap<String, String>();
    	//map.entrySet(MysqlService.class.getCanonicalName(),WebHdfsDemoControllerdownload.class.getCanonicalName());
    	//map.put("jersey.config.server.provider.classnames", WebhdfsDemoControllerupload.class.getCanonicalName());
    	//map.put("jersey.config.server.provider.classnames", MysqlService.class.getCanonicalName());
    	ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        Server jettyServer = new Server(8080);
        jettyServer.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);
       
        // Tells the Jersey Servlet which REST service/class to load.
       // jerseyServlet.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "Test1.test");
        //jerseyServlet.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "mongoDB");
        jerseyServlet.setInitParameter(ServletProperties.JAXRS_APPLICATION_CLASS, resourceConfig.class.getCanonicalName());
        /*jerseyServlet.setInitParameter(
           "jersey.config.server.provider.classnames",
           //EntryPoint.class.getCanonicalName());
           resourceConfig.class.getCanonicalName()); */
        //WebHdfsDemoControllerdownload.class.getCanonicalName());
       //jerseyServlet.setInitParameters(map);
       try {
    	   jettyServer.start();
    	   jettyServer.join();
        } 
      	catch (InterruptedException ei)
 			{
      		jettyServer.stop();
      		jettyServer.destroy();
 		}
       catch (Exception e)
       		{
           jettyServer.stop();
    	   jettyServer.destroy();
       		}
    }
}
