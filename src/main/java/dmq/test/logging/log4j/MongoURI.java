package dmq.test.logging.log4j;

import dmq.test.utils.Converter;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;

import java.util.*;


//  collection_capped   boolean(false)      是否使用“capped collection”
//  collection_size     long(1G)            “capped collection”对应的size
//  collection_max      long(1000000)       “capped collection”对应的max
//  collection_append   boolean(false)      是否使用append模式（继续使用已经存在的数据集）
//  collection_daily    boolean(false)      是否每天都重新命名一个新的数据集
//  collection_daily_format String("%s_%d_%d_%d")   每天命名新的数据集时，使用的format字符串（名称_年_月_日）
//  collection_tzid         String("CTT") 数据集的时区（统一命名时间），默认CTT，即：Asia/Shanghai
// 为了快速验证失败，扩展了原来属于MongoClientOptions的参数
//  serverSelectionTimeout  int(30000)      当无法连接mongodb的时候，缩短超时时间
public class MongoURI {
    public static final String TIMEZONE_BEIJING= "CTT";// 中国时区，CTT= Asia/Shanghai

    public static final String COLLECTION_CAPPED= "collection_capped";
    public static final String COLLECTION_SIZE= "collection_size";
    public static final String COLLECTION_MAX= "collection_max";
    public static final String COLLECTION_APPEND= "collection_append";
    // 按照日期，每天产生一个collection
    public static final String COLLECTION_DAILY= "collection_daily";
    // 每天产生一个collection时，生成名字的format字符串
    public static final String COLLECTION_DAILY_FORMAT= "collection_daily_format";
    // 规定一个统一的时区，便于命名来自不同时区的时间
    public static final String COLLECTION_TZID= "collection_tzid";
    // 为数据集指定默认索引键，多个key通常以逗号分隔
    public static final String COLLECTION_INDEX_KEYS= "collection_index_keys";
    public static final String COLLECTION_INDEX_KEYS_SEPARATOR= ",";

    // MongoClient配置参数，这些参数原本需要通过MongoClientOptions传递
    private static final String SERVER_SELECTION_TIMEOUT= "serverSelectionTimeout";

    // 从uri中识别相关配置项，并更新到builder
    private MongoClientOptions.Builder optionsBuilder= null;

    private boolean collectionCapped= false;
    private long collectionSize= 1024*1024*1024L;// 优先级高于collectionMax
    private long collectionMax= 1000000L;
    private boolean collectionAppend= false;
    private boolean collectionDaily= false;
    private String collectionDailyFormat= "%s_%d_%d_%d";// 如：default_2017_1_1
    private String collectionTzid= TIMEZONE_BEIJING;
    private List<String> collectionIndexKeys= Collections.emptyList();

    public boolean getCollectionCapped() {
        return collectionCapped;
    }
    public long getCollectionSize() {
        return collectionSize;
    }
    public long getCollectionMax() {
        return collectionMax;
    }
    public boolean getCollectionAppend() {
        return collectionAppend;
    }
    public boolean getCollectionDaily() {
        return collectionDaily;
    }
    public String getCollectionDailyFormat() {
        return collectionDailyFormat;
    }
    public String getCollectionTzid() {
        return collectionTzid;
    }
    public TimeZone getCollectionTimezone() {
        return TimeZone.getTimeZone(collectionTzid);
    }
    public List<String> getCollectionIndexKeys() {
        return collectionIndexKeys;
    }


    private final MongoClientURI clientURI;
    public MongoURI(String uri) {
        String clientUri= filterURI(uri);
        if(optionsBuilder == null)
            clientURI= new MongoClientURI(clientUri);
        else
            clientURI= new MongoClientURI(clientUri, optionsBuilder);
    }
    public MongoClientURI getMongoClientURI() {
        return clientURI;
    }

    // 从uri中过虑掉MongoClientURI不能识别的参数
    // 识别本地相关或其他相关参数，并更新应用
    private String filterURI(String uri) {
        int pos= uri.indexOf('?');
        if(pos < 0)
            return uri;

        // 在查询字符串中过虑与collection相关的配置项
        String qs= uri.substring(pos+1);
        String[] all= qs.split("&");
        List<String> remain= new ArrayList<>(all.length);
        for(String one: all) {
            if(updateLocalConfig(one))
                continue;
            if(updateClientOptions(one))
                continue;
            remain.add(one);
        }// for

        if(remain.size() == 0)
            return uri.substring(0, pos);
        return uri.substring(0, pos+1)+ String.join("&", remain);
    }
    // 查找到相匹配的本地配置项，就优先更新本地配置项
    // kv: key=value
    private boolean updateLocalConfig(String kv) {
        int pos= kv.indexOf('=');
        if(pos < 0)
            return false;

        if(pos == COLLECTION_CAPPED.length() && kv.startsWith(COLLECTION_CAPPED)) {
            collectionCapped= updateOrDefault(kv.substring(pos+1), collectionCapped);
            return true;
        } else if(pos == COLLECTION_SIZE.length() && kv.startsWith(COLLECTION_SIZE)) {
            collectionSize= updateOrDefault(kv.substring(pos+1), collectionSize);
            return true;
        } else if(pos == COLLECTION_MAX.length() && kv.startsWith(COLLECTION_MAX)) {
            collectionMax= updateOrDefault(kv.substring(pos+1), collectionMax);
            return true;
        } else if(pos == COLLECTION_APPEND.length() && kv.startsWith(COLLECTION_APPEND)) {
            collectionAppend= updateOrDefault(kv.substring(pos+1), collectionAppend);
            return true;
        } else if(pos == COLLECTION_DAILY.length() && kv.startsWith(COLLECTION_DAILY)) {
            collectionDaily= updateOrDefault(kv.substring(pos+1), collectionDaily);
            return true;
        } else if(pos == COLLECTION_DAILY_FORMAT.length() && kv.startsWith(COLLECTION_DAILY_FORMAT)) {
            collectionDailyFormat= updateOrDefault(kv.substring(pos+1), collectionDailyFormat);
            return true;
        } else if(pos == COLLECTION_TZID.length() && kv.startsWith(COLLECTION_TZID)) {
            String id= updateOrDefault(kv.substring(pos+1), collectionTzid);
            // 区分大小写，对于短ID，只识别大写字母
            if(id.length() == 3)
                collectionTzid= id.toUpperCase();
            else
                collectionTzid= id;
            return true;
        } else if(pos == COLLECTION_INDEX_KEYS.length() && kv.startsWith(COLLECTION_INDEX_KEYS)) {
            String keys= updateOrDefault(kv.substring(pos+1), "");
            collectionIndexKeys= Arrays.asList(keys.split(COLLECTION_INDEX_KEYS_SEPARATOR));
            return true;
        }
        return false;
    }
    // 查找到相匹配的MongoClient选项，就优先更新MongoClient
    // kv: key=value
    private boolean updateClientOptions(String kv) {
        int pos= kv.indexOf('=');
        if(pos < 0)
            return false;

        if(kv.startsWith(SERVER_SELECTION_TIMEOUT)) {
            int val= (int)updateOrDefault(kv.substring(pos+1), 0);
            if(val > 0)
                optionsBuilder().serverSelectionTimeout(val);
            return true;
        }
        return false;
    }
    private MongoClientOptions.Builder optionsBuilder() {
        if(optionsBuilder == null)
            optionsBuilder= new MongoClientOptions.Builder();
        return optionsBuilder;
    }


    String updateOrDefault(String config, String def) {
        if(config == null || config.length() == 0)
            return def;
        return config;
    }
    boolean updateOrDefault(String config, boolean def) {
        return Converter.parseBoolean(config, def);
    }
    long updateOrDefault(String config, long def) {
        // 默认的1K表示1024
        return Converter.parseSizeLong(config, def);
    }
    long updateOrDefault(String config, long def, int unit) {
        return Converter.parseLong(config, def, unit);
    }
}
