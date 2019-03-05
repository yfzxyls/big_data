package com.soap.design_pattern.responsiblity_chain;

/**
 * @author yangfuzhao on 2019/2/11.
 * 类描述：责任链模式测试类
 *
 */
public class Main {

    public static void main(String[] args) {

        Request request = new Request.Builder().setName("张三").setDays(1)
                .setReason("事假").build();
        ChainOfResponsibilityClient client = new ChainOfResponsibilityClient();
        Result result = client.execute(request);

        System.out.println("结果：" + result.toString());
    }
}

