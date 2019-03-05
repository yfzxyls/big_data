package com.soap.design_pattern.responsiblity_chain;

/**
 * @author yangfuzhao on 2019/2/11.
 */
public interface Ratify {

    public Result deal(Chain chain);

    interface Chain {

        /**
         * 获取请求对象，可进行加工
         * @return
         */
        Request request();

        Result proceed(Request request);
    }
}
