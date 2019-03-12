package com.soap.design_pattern.responsiblity_chain;

import java.util.List;

/**
 * @author yangfuzhao on 2019/2/11.
 */
public class RealChain implements Ratify.Chain {

    public Request request;
    public List<Ratify> ratifyList;
    public int index;

    /**
     * 构造方法
     * @param ratifyList  Ratify接口的实现类集合
     * @param request 具体的请求Request实例
     * @param index 已经处理过该request的责任人数量
     */
    public RealChain(List<Ratify> ratifyList, Request request, int index) {
        this.ratifyList = ratifyList;
        this.request = request;
        this.index = index;
    }


    @Override
    public Request request() {
        return request;
    }

    @Override
    public Result proceed(Request request) {
        Result proceed = null;
        if (ratifyList.size() > index) {
            RealChain realChain = new RealChain(ratifyList, request, index + 1);
            Ratify ratify = ratifyList.get(index);
            proceed = ratify.deal(realChain);
        }
        return proceed;
    }
}
