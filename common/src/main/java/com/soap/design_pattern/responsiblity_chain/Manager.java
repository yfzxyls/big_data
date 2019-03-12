package com.soap.design_pattern.responsiblity_chain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yangfuzhao on 2019/2/11.
 */
public class Manager implements Ratify {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    @Override
    public Result deal(Chain chain) {
        Request request = chain.request();
        LOGGER.info("Manager=====>request:" + request.toString());
        if (request.days() > 3) {
            // 构建新的Request
            Request newRequest = new Request.Builder().newRequest(request)
                    .setManagerInfo(request.name() + "每月的KPI考核还不错，可以批准")
                    .build();
            return chain.proceed(newRequest);
        }
        return new Result(true, "Manager：早点把事情办完，项目离不开你");
    }
}
