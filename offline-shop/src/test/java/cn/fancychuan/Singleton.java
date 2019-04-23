package java.cn.fancychuan;

public class Singleton {
    private static Singleton instance = null;

    public Singleton() {
    }

    /**
     * 需要考虑多线程并发访问的安全问题
     *
     * 为什么不用 public static synchronized Singleton getInstance() {} ??
     * 这是因为第一次访问的时候确实可以避免多个线程并发访问创建多个实例的问题，但是之后就会导致在方法级别进行同步，从而使并发性能大幅度降低
     */
    public static Singleton getInstance() {
        // 二步检查机制
        // 1. 多个线程过来先判断是否为null
        if (instance == null) {
            // 2. 为null了以后才需要进行同步，这个时候只有一个线程能够获取到Singleton Class对象的锁
            synchronized (Singleton.class) {
                if (instance == null) {
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }
}
