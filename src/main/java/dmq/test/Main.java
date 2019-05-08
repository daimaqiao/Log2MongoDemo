package dmq.test;

import dmq.test.logging.Log2Mongo;
import dmq.test.utils.Local;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final String LOG2MONGO_URI= Log2Mongo.LOG2MONGO_URI;
    private static final String DEMO_URI= "mongodb://localhost/database.collection?collection_append=false";
    private static final String DEMO_RUN= "java -jar";
    private static final String DEMO_JAR= "<Log2MongoDemo-xxx.jar>";

    private static final Logger LOGGER= LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        String mongoUri= System.getProperty(LOG2MONGO_URI);
        if(mongoUri == null) {
            if(args.length > 0) {
                mongoUri= args[0];
                System.setProperty(LOG2MONGO_URI, mongoUri);
            } else {
                mongoUri= "";
            }
        }

        if(!mongoUri.startsWith("mongodb://")) {
            String pkg= Local.getAppName();
            if(pkg == null || !pkg.endsWith(".jar"))
                pkg= DEMO_JAR;

            System.out.println(String.format("ERROR: Bad property %s: %s", LOG2MONGO_URI, mongoUri));
            System.out.println();
            System.out.println(String.format("USAGE: %s %s <%s>", DEMO_RUN, pkg, LOG2MONGO_URI));
            System.out.println(String.format("eg. %s %s %s", DEMO_RUN, pkg, DEMO_URI));
            System.out.println();
            return;
        }

        Log2Mongo log2Mongo= new Log2Mongo(mongoUri, Level.INFO);
        log2Mongo.appendMongoAppender();

        LOGGER.debug("Print property: {} = {}", LOG2MONGO_URI, mongoUri);

        LOGGER.info("Demo message from SimpleLogging 1.");
        LOGGER.warn("Demo message from SimpleLogging 2.");
        LOGGER.error("Demo message from SimpleLogging 3.");

        log2Mongo.removeMongoAppender();
        System.out.println();
        System.out.println(" === end === ");
    }
}
