package dmq.test.logging.log4j;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.stream.Collectors;

//  1. writeSaveTime(true)      是否在插入数据的时候，记录当前时间标签（SAVE_TIME）。默认true，可关闭
//  2. putExtraElement(String, Object)  是插入数据的时候，可以添加额外的键名与键值（添加到存档JSON中）
//  3. ignoreException(false)   在构造mongodb连接时，确认连接可用性并忽略底层异常（配置为放弃数据）。默认false，用于构造函数
public class MongoSink {
    private static final Logger LOGGER= LoggerFactory.getLogger(MongoSink.class);

    private static final String DATABASE_NAME= "log2mongo";
    private static final String COLLECTION_NAME= "default";
    private static final String SAVE_TIME= "savetime";

    // 创建数据集时，已经存在，抛出MongoCommandException异常
    private static final int ERROR_COLLECTION_EXIST= 48;

    private MongoClient mongoClient= null;
    private MongoDatabase mongoDatabase= null;
    private MongoCollection mongoCollection= null;

    // 是否在插入数据时候，同时添加一个字段，记录插入数据的时间
    private boolean writeSaveTime= true;
    public void disableSaveTime() {
        writeSaveTime= false;
    }

    // 添加额外的JSON信息(key=value)到每条输出的日志中
    private BasicBSONObject extraElements= new BasicBSONObject();
    public void putExtraElement(String key, Object val) {
        extraElements.put(key, val);
    }

    // 重命名SAVE_TIME对应的字段名
    private String saveTime= SAVE_TIME;
    public void renameSaveTime(String name) {
        if(name != null && name.length() > 0)
            saveTime= name;
    }
    // MongoURI中定义了一个数据集时区，数据集时区可用于统一数据集内的时间信息
    public TimeZone getCollectionTimezone() {
        return dailyCollectionTimezone;
    }

    // 为了简化逻辑，暂时只考虑单台主机，并且无需认证的情况
    private final MongoURI mongoURI;
    private final MongoClientURI clientURI;
    // uri使用MongoURI支持的扩展形式，即在标准的uri之后，可以附加关于collection的若干参数
    // 如：mongodb://192.168.0.1:27017/MongoSink.test?collection_append=true
    private final boolean dailyCollection;
    private final String dailyCollectionFormat;
    private final TimeZone dailyCollectionTimezone;
    private final List<String> mongoIndexKeys;
    public MongoSink(String uri) {
        this(uri, false);
    }
    // 强制在打开mongodb之后确认连接可用性，可忽略连接异常（放弃数据）后继续
    // 虽然解析uri的过程中也有可能抛出异常，但是这个属于配置失误，需要重新配置之后继续
    public MongoSink(String uri, boolean ignoreException) {
        mongoURI= new MongoURI(uri);
        clientURI= mongoURI.getMongoClientURI();
        dailyCollection= mongoURI.getCollectionDaily();
        dailyCollectionFormat= mongoURI.getCollectionDailyFormat();
        dailyCollectionTimezone= mongoURI.getCollectionTimezone();
        mongoIndexKeys= mongoURI.getCollectionIndexKeys();
        dailyCollectionInit();// 每天一个记录集的初始化要在打开之前初始化
        if(ignoreException)
            ensureOpen();
        else
            open();
        connectNow();
    }
    // 确认mongodb可用。MongoClient只在必要的时候连接数据库
    // 如果不考虑性能问题的话，可重复调用，用于确认mongodb可用
    // ensureOpen会间接调用open
    private boolean ensureOpen() {
        try {
            if(mongoClient == null || mongoDatabase == null || mongoCollection == null) {
                close();
                open();
            }
            // 做这么多查询，可以强制连接数据库吧:-D
            //System.out.println("list database names: ");
            //mongoClient.listDatabaseNames().forEach((Block<String>)System.out::println);
            LOGGER.trace("list database names: ");
            mongoClient.listDatabaseNames().forEach((Consumer<String>)LOGGER::trace);
            //System.out.println("list collection names: ");
            //mongoDatabase.listCollectionNames().forEach((Block<String>)System.out::println);
            LOGGER.trace("list collection names: ");
            mongoDatabase.listCollectionNames().forEach((Consumer<String>)LOGGER::trace);
            long count= mongoCollection.countDocuments();
            //System.out.printf("count documents: %d\n", count);
            LOGGER.trace("count documents: {}", count);
            return true;
        } catch(Exception x) {
            x.printStackTrace();
            //System.out.println(" *** WARN *** Database unavailable, data will be dropped!");
            LOGGER.warn(" *** WARN *** Database unavailable, data will be dropped!");
            close();
        }
        return false;
    }

    // 在初始化的时候打开mongodb
    private void open() {
        assert mongoClient == null: "MongoSink should be opened only once!";
        mongoClient= new MongoClient(clientURI);
        mongoDatabase= mongoClient.getDatabase(databaseName());
        initCollection();
    }
    private void connectNow() {
        if(mongoClient == null || mongoDatabase == null || mongoCollection == null)
            return;
        // 查询一次数据库名应该就可以了吧^o^
        mongoClient.listDatabaseNames();
    }
    private String databaseName() {
        String db= clientURI.getDatabase();
        if(db == null || db.length() == 0)
            return DATABASE_NAME;
        return db;
    }
    private String collectionName() {
        String c= clientURI.getCollection();
        if(c == null || c.length() == 0)
            return COLLECTION_NAME;
       return c;
    }
    private MongoCollection getCurrentCollection() {
        return mongoCollection;
    }
    // 初始化数据集，根据“MongoURI”中额外的相关参数处理数据集
    private void initCollection() {
        String collectionName;
        if(dailyCollection && dailyCollectionName != null && dailyCollectionName.length() > 0) {
            collectionName= dailyCollectionName;
        } else
            collectionName= collectionName();

        if(!mongoURI.getCollectionAppend()) {
            // 关闭append功能，意味着每次初始化之前先清除已经存在的数据集
            mongoDatabase.getCollection(collectionName).drop();
        }

        if(mongoURI.getCollectionCapped()) {
            // 打开capped功能，意味着每次都要尝试创建“Capped Collection”的数据集
            // 当相同的数据集已经存在时，会抛出异常
            try {
                CreateCollectionOptions options = new CreateCollectionOptions();
                options.capped(true);
                options.sizeInBytes(mongoURI.getCollectionSize());
                options.maxDocuments(mongoURI.getCollectionMax());
                mongoDatabase.createCollection(collectionName, options);
            } catch(MongoCommandException x) {
                // 忽略数据集已经存在的问题，关闭append功能可以自动删除已经存在的数据集
                if(x.getErrorCode() != ERROR_COLLECTION_EXIST) {
                    x.printStackTrace();
                    throw x;
                }
            }
        }
        mongoCollection= mongoDatabase.getCollection(collectionName);
        mongoIndexKeys.forEach(x-> mongoCollection.createIndex(new BasicDBObject(x, 1)));
    }

    // 检查并更新按天命名的数据集
    private String dailyCollectionName= null;   // 当前记录集名称
    private long dailyCollectionTime= 0;        // 当前记录集名称对应的时间（每天都要更换记录集名称）
    private int timezoneDelta= 0;               // 当前记录集名称相应的时间的时区偏差
    private final long timeOfDay= 24*3600*1000;
    private synchronized void dailyCollectionInit() {
        Calendar calendar= Calendar.getInstance(dailyCollectionTimezone);
        if(forceCalendarFlag)
            calendar= forceCalendar;

        dailyCollectionName= String.format(dailyCollectionFormat,
                collectionName(),
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH)+1,// 月份是0~11
                calendar.get(Calendar.DAY_OF_MONTH));
        timezoneDelta= calendar.getTimeZone().getRawOffset();   // 在时区一致的情况下才可以比较时间
        dailyCollectionTime= (calendar.getTimeInMillis()+timezoneDelta)/timeOfDay;  // 同一天内的时间取整之后是相同的
    }
    // 严格地来说，这是一个测试
    private boolean forceCalendarFlag= false;// 进入测试模式
    private Calendar forceCalendar= null;
    synchronized void forceCalendar(Calendar calendar) {
        // 进入测试模式
        forceCalendarFlag = true;
        if (calendar != null)
            forceCalendar = calendar;
        else {
            // 从测试模式中恢复，需要重新初始化dailyCollection
            forceCalendar = Calendar.getInstance(dailyCollectionTimezone);
            forceCalendar.setTimeInMillis(0);
        }
        // 强制更新
        dailyCollectionInit();

        if (calendar == null) {
            // 退出测试模式
            forceCalendarFlag = false;
            forceCalendar = null;
        }
    }
    private synchronized void dailyCollectionUpdate() {
        if(!dailyCollection)
            return;
        // 考虑到（测试的时候）系统的时间有可能向后调整，因此时间值有从大变小的可能，所以不能比大小
        // 如果前后时间不在同一个时区，则需要强制更新登记的时间时区信息
        Calendar calendar= Calendar.getInstance(dailyCollectionTimezone);
        int tzDelta= calendar.getTimeZone().getRawOffset();
        long dailyTime= (System.currentTimeMillis()+tzDelta)/timeOfDay;
        LOGGER.trace("dailyCollectionUpdate -- timezone: {}({}), dailyTime: {}({})",
                tzDelta, timezoneDelta, dailyTime, dailyCollectionTime);
        if(tzDelta == timezoneDelta && dailyTime == dailyCollectionTime)
            return;

        // 换天了，更换记录集名称
        dailyCollectionInit();

        // 重新初始化记录集
        initCollection();
    }

    // 逐个写入数据，写入方法是同步完成的
    private void write(Document doc) {
        if(mongoClient == null || mongoDatabase == null || mongoCollection == null) {
            //System.out.println("MongoSink: dropped data!");
            LOGGER.trace("MongoSink: dropped data!");
            return;
        }

        try {
            preWrite(doc);
            dailyCollectionUpdate();
            mongoCollection.insertOne(doc);
        } catch(Exception x) {
            x.printStackTrace();
        }
    }
    // 批量写入数据，写入方法是同步完成的
    private void write(List<Document> list) {
        if(mongoClient == null || mongoDatabase == null || mongoCollection == null) {
            //System.out.printf("MongoSink: dropped data in bulk! (size= %d)\n", list.size());
            LOGGER.trace("MongoSink: dropped data in bulk! (size= {})", list.size());
            return;
        }

        try {
            list.forEach(x-> preWrite(x));
            dailyCollectionUpdate();
            mongoCollection.insertMany(list);
        } catch(Exception x) {
            x.printStackTrace();
        }
    }
    private void preWrite(Document doc) {
        if(writeSaveTime)
            doc.put(saveTime, new Date());
        doc.putAll(extraElements);
    }


    // 向mongodb中写入数据
    public void write(BSONObject object) {
        Document doc = new Document(object.toMap());
        write(doc);
    }
    public void message(String string) {
        Document document= new Document("message", string);
        write(document);
    }
    public void writeJson(String json) {
        Document doc = Document.parse(json);
        write(doc);
    }
    // 向mongodb中写入批量数据
    public void writeList(List<BSONObject> list) {
        List<Document> docList= (List<Document>)list.stream()
                .map(x-> new Document(x.toMap()))
                .collect(Collectors.toList());
        write(docList);
    }
    public void message(List<String> list) {
        List<Document> docList= (List<Document>)list.stream()
                .map(x-> new Document("message", x))
                .collect(Collectors.toList());
        write(docList);
    }
    public void writeJson(List<String> list) {
        List<Document> docList= (List<Document>)list.stream()
                .map(x-> Document.parse(x))
                .collect(Collectors.toList());
        write(docList);
    }


    // 关闭之后，当前实例就废弃了
    public void close() {
        LOGGER.debug("Close mongo sink .");
        if(mongoClient != null) {
            mongoClient.close();
            mongoClient= null;
        }
        mongoDatabase= null;
        mongoCollection= null;
    }
}
