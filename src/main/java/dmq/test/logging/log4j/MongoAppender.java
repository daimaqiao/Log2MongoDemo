package dmq.test.logging.log4j;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.List;


//  1. putExtraElement(String, Object)  是插入数据的时候，可以添加额外的键名与键值（添加到存档JSON中）
//  2. ignoreException(false)   在构造mongodb连接时，确认连接可用性并忽略底层异常（配置为放弃数据）。默认false，用于构造函数
public class MongoAppender extends AppenderSkeleton
        implements TimedBuffer.BufferHandler<BSONObject>, TimedBuffer.BufferDroppedNotify<BSONObject> {
    private static final Logger LOGGER= LoggerFactory.getLogger(MongoAppender.class);

    private static final int MAX_THROWABLE= 5;// throwable最大嵌套层次

    // mongodb驱动程序包前缀
    private static final String MONGO_PREFIX= "org.mongodb.";

    // 缓存多一些的时候，IO性能会好一些
    private static final int SIZE_IN_BULK= TimedBuffer.THRESHOLD_SIZE;  // 批量写入的数量，默认1000
    private static final int TIME_IN_BULK= TimedBuffer.THRESHOLD_TIME;  // 定时写入的周期，默认1000 (ms)
    private static final int SIZE_BUFFER= TimedBuffer.MAX_CAPACITY;     // 最大缓存数量，默认10000，超出的数据被丢弃
    private static final int SIZE_POOL= TimedBuffer.MAX_THREADS;        // 定时服务线程池大小
    private static final boolean PER_DELAY= TimedBuffer.FIX_DELAY;      // 使用两次调用之间的间隔时间计算延时

    private MongoSink mongoSink;
    private TimedBuffer<BSONObject> timedBuffer;
    private final boolean usingTimedBuffer;// 是否启用timedBuffer，默认true
    private final Calendar defaultCalendar;// 默认日历，根据MongoSink的timezone设置默认日历
    // 当mongoSink的写入平均效率低于logging的平均执行效率时，notifyBufferDroppped应该会有所通报
    public MongoAppender(String uri, boolean usingTimedBuffer, boolean ignoreException) {
        this.usingTimedBuffer= usingTimedBuffer;
        setName(this.getName());
        mongoSink= new MongoSink(uri, ignoreException);
        defaultCalendar= Calendar.getInstance(mongoSink.getCollectionTimezone());
        if(usingTimedBuffer)
            timedBuffer= new TimedBuffer<BSONObject>(SIZE_BUFFER, SIZE_IN_BULK, TIME_IN_BULK, SIZE_POOL, PER_DELAY, this, this);
        else
            timedBuffer= null;
    }
    public MongoAppender(String uri) {
        // 默认打开TimedBuffer，忽略MongoSink的异常
        this(uri, true, true);
    }

    public void putExtraElement(String key, Object val) {
        mongoSink.putExtraElement(key, val);
    }
    public void renameSaveTime(String name) {
        mongoSink.renameSaveTime(name);
    }


    private boolean filterEvent(LoggingEvent loggingEvent) {
        String name= loggingEvent.getLoggerName();
        return name.startsWith(MONGO_PREFIX);
    }


    // 只转换一些常用的信息
    private String formatTime(long time) {
        defaultCalendar.setTimeInMillis(time);
        int offset= defaultCalendar.get(Calendar.ZONE_OFFSET);
        return String.format("%4d-%02d-%02d %02d:%02d:%02d %+03d%02d",
                defaultCalendar.get(Calendar.YEAR),
                defaultCalendar.get(Calendar.MONTH)+1,// 月份是0~11
                defaultCalendar.get(Calendar.DAY_OF_MONTH),
                defaultCalendar.get(Calendar.HOUR_OF_DAY),
                defaultCalendar.get(Calendar.MINUTE),
                defaultCalendar.get(Calendar.SECOND),
                offset/3600000,
                Math.abs(offset)%3600000/60000);
    }

    private BSONObject formatEvent(LoggingEvent event) {
        BasicBSONObject object= new BasicBSONObject();
        object.put("level", event.getLevel().toString());
        object.put("message", event.getMessage());

        object.put("timestamp", new Date(event.getTimeStamp()));
        object.put("timetext", formatTime(event.getTimeStamp()));

        LocationInfo info= event.getLocationInformation();
        object.put("file", info.getFileName());
        object.put("line", info.getLineNumber());
        object.put("class", info.getClassName());
        object.put("method", info.getMethodName());

        object.put("exception", formatThrowable(event.getThrowableInformation()));

        return object;
    }
    private BSONObject formatThrowable(ThrowableInformation info) {
        if(info == null)
            return null;

        BasicBSONObject object= new BasicBSONObject();
        String[] array= info.getThrowableStrRep();
        object.put("trace", array==null?null: String.join("\n", array));

        Throwable throwable= info.getThrowable();
        if(throwable != null) {
            object.put("name", throwable.getClass().getCanonicalName());
            object.put("message", throwable.getMessage());

            object.put("cause", formatThrowable(throwable.getCause(), 0));
        }

        return object;
    }
    private BSONObject formatThrowable(Throwable throwable, int level) {
        if(throwable == null)
            return null;
        BasicBSONObject object= new BasicBSONObject();
        if(level < MAX_THROWABLE) {
            object.put("name", throwable.getClass().getCanonicalName());
            object.put("message", throwable.getMessage());
            object.put("cause", formatThrowable(throwable.getCause(), level+1));
        } else {
            object.put("name", "discarded");
            object.put("message", String.format("Cause level more than %d will be discarding.", level));
            object.put("cause", null);
        }

        return object;
    }

    @Override// AppenderSkeleton
    protected void append(LoggingEvent loggingEvent) {
        // 过虑掉不相关的日志
        if(filterEvent(loggingEvent))
            return;

        BSONObject object= formatEvent(loggingEvent);
        // TimedBuffer可以改善连续向mongodb写入数据的性能
        if(usingTimedBuffer)
            timedBuffer.put(object);
        else
            mongoSink.write(object);
    }
    @Override// AppenderSkeleton
    public void close() {
        LOGGER.debug("Close mongo appender .");
        if(timedBuffer != null)
            timedBuffer.close(()-> mongoSink.close());
        else
            mongoSink.close();
    }
    @Override// AppenderSkeleton
    public boolean requiresLayout() {
        return false;
    }


    @Override// TimedBuffer.BufferHandler<BSONObject>
    public void processBuffer(List<BSONObject> bufferList) {
        //System.out.printf("processBuffer: writing in bulk (count= %d)\n", bufferList.size());
        LOGGER.trace("processBuffer: writing in bulk (count= {})", bufferList.size());
        // 使用timedBuffer批量写入数据
        mongoSink.writeList(bufferList);
    }

    @Override// TimedBuffer.BufferDroppedNotify<BSONObject>
    public void notifyBufferDroppped(long count) {
        String message= String.format("notifyBufferDropped: something dropped by the buffer. full? (count= %d)", count);
        //System.out.println(message);
        LOGGER.trace(message);
        // 用于改善服务，通告缓存过程中出现过过载的的信息
        mongoSink.message(message);
    }
}
