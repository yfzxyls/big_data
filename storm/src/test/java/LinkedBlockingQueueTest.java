import org.apache.storm.shade.org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author yangfuzhao on 2018/11/26.
 */
public class LinkedBlockingQueueTest {
    BlockingQueue<Integer> blockingQueue = new LinkedBlockingQueue();

    @Test
    public void test() {


        new Thread(new Runnable() {
            public void run() {
                while (true){
                    Integer poll = null;
                    try {
                        poll = blockingQueue.poll(3, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName()+poll);
                }
            }
        }).start();
        new Thread(new Runnable() {
            public void run() {
                while (true){
                    Integer poll = null;
                    try {
                        poll = blockingQueue.poll(1, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName()+poll);
                }
            }
        }).start();

        for (int i = 0; i < 1000; i++) {
            blockingQueue.add(i);

            try {
                Thread.sleep(RandomUtils.nextInt(10) * 100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    @Before
    public void before() {

    }

    @Test
    public void ran(){
        int i = RandomUtils.nextInt(10);
        System.out.println(i);
    }
}
