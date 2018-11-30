package com.soap.common.thread;

import java.util.concurrent.CountDownLatch;

public class CountDownTest {

    static CountDownLatch c = new CountDownLatch(2);


    public static void main(String[] args) throws InterruptedException {

        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("done1");

                c.countDown();
            }
        }).start();

        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("done2");
                c.countDown();
            }
        }).start();

        c.await();

        System.out.println("all done");
    }
}
