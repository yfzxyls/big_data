package com.soap.common.aspect;

/**
 * @author yangfuzhao on 2019/1/11.
 */
public class HelloWorld {

    public void sayHello(){
        System.out.println("hello world !");
    }

    public static void main(String[] args) {
        HelloWorld helloWorld = new HelloWorld();
        helloWorld.sayHello();
    }
}


