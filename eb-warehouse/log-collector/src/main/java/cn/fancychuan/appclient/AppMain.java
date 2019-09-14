package cn.fancychuan.appclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 日志行为数据模拟
 *
 * 需要有两个参数：
 *  1. 控制发送每条数据的延迟时间，默认是0s
 *  2. 循环遍历次数，默认是1000
 *
 * 经验：
 *  1. 开发某一个程序的时候都是需要好好考虑可配置化的！
 *
 */
public class AppMain {
    private final static Logger logger = LoggerFactory.getLogger(AppMain.class);
    private static Random rand = new Random();
    // 设备id
    private static int s_mid = 0;
    // 用户id
    private static int s_uid = 0;
    // 商品id
    private static int s_goodsid = 0;

    public static void main(String[] args) {
        Long delay = args.length > 0 ? Long.parseLong(args[0]) : 0L;
        Long loop_len = args.length > 0 ? Long.parseLong(args[0]) : 1000;

    }

    private static void generateLog(Long delay, int loop_len) {
        for (int i = 0; i < loop_len; i++) {
            int flag = rand.nextInt(2);
        }
    }
}
