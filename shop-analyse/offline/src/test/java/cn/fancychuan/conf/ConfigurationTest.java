package cn.fancychuan.conf;

public class ConfigurationTest {

    public static void main(String[] args) {
        System.out.println(ConfigurationManager.getProperty("key1"));
        System.out.println(ConfigurationManager.getProperty("key2"));
        System.out.println(ConfigurationManager.getProperty("key3"));
    }
}
