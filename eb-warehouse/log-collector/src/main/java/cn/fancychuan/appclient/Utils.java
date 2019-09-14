package cn.fancychuan.appclient;

import java.util.Random;

public class Utils {
    /**
     * 获取指定长度的随机字母组合
     */
    public static String getRandomChar(Integer length) {
        StringBuilder str = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            str.append((char) (65 + random.nextInt(26)));// 取得大写字母
        }
        return str.toString();
    }
    /**
     * 获取随机字母数字组合
     */
    public static String getRandomCharAndNumr(Integer length) {
        StringBuilder str = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            boolean b = random.nextBoolean();
            if (b) { // 字符串
                // int choice = random.nextBoolean() ? 65 : 97; 取得65大写字母还是97小写字母
                str.append((char) (65 + random.nextInt(26)));// 取得大写字母
            } else { // 数字
                str.append(String.valueOf(random.nextInt(10)));
            }
        }
        return str.toString();
    }

}
