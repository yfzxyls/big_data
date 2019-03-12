package com.soap.design_pattern.responsiblity_chain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yangfuzhao on 2019/2/11.
 */
public class DepartmentHeader implements Ratify {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @Override
    public Result deal(Chain chain) {

        Request request = chain.request();
        LOGGER.info("DepartmentHeader=====>request:{}",request);
        if (request.days() > 7) {
            return new Result(false, "你这个完全没必要");
        }
        return new Result(true, "DepartmentHeader：不要着急，把事情处理完再回来！");
    }
}
