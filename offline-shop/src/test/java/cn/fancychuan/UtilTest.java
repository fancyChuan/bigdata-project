package cn.fancychuan;

import cn.fancychuan.util.DateUtils;

public class UtilTest {

    public static void main(String[] args) {
        testRandomDate();
    }

    public static void testRandomDate() {
        for (int i=0; i<20; i++) {
            System.out.println(DateUtils.getRandomDate());
        }

    }
}
