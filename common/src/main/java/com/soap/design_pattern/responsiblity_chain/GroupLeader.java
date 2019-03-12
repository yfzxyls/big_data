package com.soap.design_pattern.responsiblity_chain;




import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yangfuzhao on 2019/2/11.
 */
public class GroupLeader implements Ratify {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupLeader.class);

    @Override
    public Result deal(Chain chain) {
        Request request = chain.request();
        LOGGER.info("GroupLeader=====>request:" + request.toString());
        if (request.days() > 1) {
            // 包装新的Request对象
            Request newRequest = new Request.Builder().newRequest(request)
                    .setManagerInfo(request.name() + "平时表现不错，而且现在项目也不忙")
                    .build();
            return chain.proceed(newRequest);
        }
        return new Result(true, "GroupLeader：早去早回");
    }
}
