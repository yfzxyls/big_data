package com.soap.common.io;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * 管道IO
 * 只能在两个线程中使用，否则会形成死锁
 */
public class PipIo {
    public static void main(String[] args) throws IOException{


        final PipedOutputStream outputStream = new PipedOutputStream();

        final PipedInputStream inputStream = new PipedInputStream(outputStream);
//        IOUtils.copy()

//
//        Thread thread = new Thread(new Runnable() {
//            public void run() {
//                try {
//                    outputStream.write("hello io".getBytes());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }) {
//        };
//
//
//        Thread thread1 = new Thread(new Runnable() {
//            public void run() {
//
//                try {
//                    int read = inputStream.read();
//                    while (read != -1) {
//                        System.out.print((char)read);
//                        read = inputStream.read();
//                    }
//                } catch (IOException e) {
//                }
//
//
//            }
//        }) {
//        };
//
//
//        thread.start();
//        thread1.start();
        int read = inputStream.read();
        while (read != -1){
            System.out.println((char)read);
            read = inputStream.read();
        }

        outputStream.write("hello io".getBytes());


    }
}
