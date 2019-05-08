package dmq.test.utils;

import java.util.ArrayList;
import java.util.Collection;


public class Converter {
    // 解析字符串为true/false
    public static boolean parseBoolean(String str, boolean def) {
        if(str == null || str.isEmpty())
            return def;
        switch(str.toLowerCase()) {
            case "true":
            case "1":
            case "yes":
            case "ok":
            case "enabled":
            case "open":
                return true;
            case "false":
            case "0":
            case "no":
            case "disabled":
            case "close":
                return false;
            default:
                try {
                    return Integer.parseInt(str) != 0;
                } catch(Exception x) {
                    x.printStackTrace();
                }
        }// switch
        return def;
    }


    // 解析字符串为整数
    public static int parseInt(String str, int def) {
        if(str == null || str.isEmpty())
            return def;
        char ch= str.charAt(0);
        if(ch < '0' || ch > '9')
            return def;
        try {
            return Integer.parseInt(str);
        } catch(Exception x) {
            x.printStackTrace();
        }
        return def;
    }


    // 解析字符串为整数，1k=1000
    public static long parseNumLong(String str, long def) {
        return parseLong(str, def, 1000);
    }
    // 解析字符串为整数，1k=1024
    public static long parseSizeLong(String str, long def) {
        return parseLong(str, def, 1024);
    }
    // 解析字符串为整数，unit指定单位1k代表多少（1000/1024）
    public static long parseLong(String str, long def, int unit) {
        if(str == null || str.isEmpty())
            return def;
        try {
            char last= str.charAt(str.length()-1);
            if(last >= '0' && last <= '9')
                return Long.parseLong(str);

            // 识别常见数字后缀：K, M, G, T
            // 识别自定义后缀：W(万, 10K), Y(亿, 10M)
            String number= str.substring(0, str.length()-1);
            long value= Long.parseLong(number);
            switch(last) {
                case 'K':
                case 'k':
                    return value*unit;
                case 'M':
                case 'm':
                    return value*unit*unit;
                case 'G':
                case 'g':
                    return value*unit*unit*unit;
                case 'T':
                case 't':
                    return value*unit*unit*unit*unit;
                case 'W':
                case 'w':
                    return value*unit*10;
                case 'Y':
                case 'y':
                    return value*unit*unit*100;
                case 'L':
                case 'l':
                    return value;
            }// switch
        } catch(Exception x) {
            x.printStackTrace();
        }
        return def;
    }


    public static String formatNumLong(long val) {
        final String K= "000", M= "000000", G= "000000000", T= "000000000000";
        final String W= "0000", Y= "00000000";

        String str= Long.toString(val);
        if(str.endsWith(T))
            return str.substring(0, str.length()-T.length())+"t";
        if(str.endsWith(G))
            return str.substring(0, str.length()-G.length())+"g";
        if(str.endsWith(Y))
            return str.substring(0, str.length()-Y.length())+"y";
        if(str.endsWith(W))
            return str.substring(0, str.length()-W.length())+"w";
        if(str.endsWith(M))
            return str.substring(0, str.length()-M.length())+"m";
        if(str.endsWith(K))
            return str.substring(0, str.length()-K.length())+"k";
        return str;
    }


    public static <T> ArrayList<T> ensureArrayList(Collection<T> collection) {
        if(collection == null || collection instanceof ArrayList)
            return (ArrayList<T>) collection;
        return new ArrayList<>(collection);
    }
}
