package dmq.test.logging;

import dmq.test.logging.log4j.MongoAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

//示例：log2mongo_uri=mongodb://192.168.0.1:27017/database.collection
public class Log2Mongo {
    public static final String LOG2MONGO_URI= "log2mongo_uri";

    private final Level level;
    private final String uri;
    public Log2Mongo(String uri, Level level) {
        this.uri= uri;
        this.level= level;
    }
    public Log2Mongo(Level level) {
        this(System.getProperty(LOG2MONGO_URI, ""), level);
    }

    // 将实例化的MongoAppender添加到log4j的RootLogger
    private MongoAppender mongoAppender= null;
    public MongoAppender appendMongoAppender() {
        if(uri.length() == 0)
            return null;

        if(mongoAppender == null) {
            mongoAppender = new MongoAppender(uri);
            mongoAppender.setName(uri);
            mongoAppender.setThreshold(level);
            //mongoAppender.putExtraElement("hello", "log2mongo_test");
            //mongoAppender.renameSaveTime("save_time");
            Logger.getRootLogger().addAppender(mongoAppender);
        }
        return mongoAppender;
    }

    public void removeMongoAppender() {
        if(mongoAppender != null) {
            Logger.getRootLogger().removeAppender(mongoAppender);
            mongoAppender.close();
            mongoAppender = null;
        }
    }

    public String dump() {
        return String.format("level=%s, uri=%s",
                level.toString(), uri);
    }

}
