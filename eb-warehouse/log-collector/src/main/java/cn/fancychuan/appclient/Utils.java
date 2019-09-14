package cn.fancychuan.appclient;

import java.io.UnsupportedEncodingException;
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

    /**
     * 生成单个汉字
     */
    public static char getRandomChar() {
        String str = "";
        int hightPos; //
        int lowPos;

        Random random = new Random();
        //随机生成汉子的两个字节
        hightPos = (176 + Math.abs(random.nextInt(39)));
        lowPos = (161 + Math.abs(random.nextInt(93)));

        byte[] b = new byte[2];
        b[0] = (Integer.valueOf(hightPos)).byteValue();
        b[1] = (Integer.valueOf(lowPos)).byteValue();

        try {
            str = new String(b, "GBK");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.out.println("错误");
        }

        return str.charAt(0);
    }

    /**
     * 拼接成多个汉字
     */
    public static String getCONTENT() {
        StringBuilder str = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i <random.nextInt(100); i++) {
            str.append(getRandomChar());
        }
        return str.toString();
    }
}
