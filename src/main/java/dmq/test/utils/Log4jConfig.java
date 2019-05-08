package dmq.test.utils;

import org.apache.log4j.*;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class Log4jConfig {
    public static final String CONFIG_DIR= "config";
    public static final String CONFIG_NAME= "log4j.properties";

    public static String loadExternalConfig() {
        if(Local.checkAppConf(CONFIG_NAME))
            return Local.getAppConf(CONFIG_NAME).toString();

        String name= String.format("%s/%s", CONFIG_DIR, CONFIG_NAME);
        if(Local.checkAppConf(name))
            return Local.getAppConf(name).toString();

        return null;
    }

    public static String useExternalConfig() {
        String config= loadExternalConfig();
        if(config != null) {
            PropertyConfigurator.configure(config);

        }

        return config;
    }

}
