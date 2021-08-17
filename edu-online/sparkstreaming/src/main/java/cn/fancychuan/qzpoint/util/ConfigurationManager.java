package cn.fancychuan.qzpoint.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {

    private static Properties prop = new Properties();

    static {
        try {
            InputStream inputStream = ConfigurationManager.class.getClassLoader()
                    .getResourceAsStream("comerce.properties");
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }


    public static boolean getBoolean(String key) {
        String value = prop.getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


}
