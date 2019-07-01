package other;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 使用线程池来spark每次只能有一个作业在运行 TODO：待完善
 */
public class ExecutorThreadPool {
    public static void main(String[] args) {
        ExecutorService threadPool = Executors.newFixedThreadPool(1);

        threadPool.submit(new Runnable() {
            @Override
            public void run() {

            }
        });
    }

}
