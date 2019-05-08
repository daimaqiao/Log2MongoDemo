package dmq.test.utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Local {
    private static Path localAppPath() {
        try {
            return Paths.get(Log4jConfig.class.getProtectionDomain().getCodeSource().getLocation().toURI());

        } catch (Exception x) {
            x.printStackTrace();
        }
        return Paths.get(".").toAbsolutePath();
    }
    private static Path appPath= null;
    public static Path getAppPath() {
        if(appPath == null)
            appPath= localAppPath();
        return appPath;
    }

    private static Path appDir= null;
    public static Path getAppDir() {
        if(appDir == null)
            appDir= getAppPath().getParent();
        return appDir;
    }

    private static String appName= null;
    public static String getAppName() {
        if(appName == null)
            appName= getAppPath().getFileName().toString();
        return appName;
    }

    public static Path getAppConf(String name) {
        return getAppDir().resolve(name);
    }

    public static boolean checkAppConf(String name) {
        return Files.exists(getAppConf(name));
    }

}
