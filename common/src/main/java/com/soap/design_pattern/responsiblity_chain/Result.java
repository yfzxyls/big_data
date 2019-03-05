package com.soap.design_pattern.responsiblity_chain;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author yangfuzhao on 2019/2/11.
 * 类描述：结果对象
 */
public class Result {
    public boolean isRatify;
    public String info;

    public Result() {

    }

    public Result(boolean isRatify, String info) {
        super();
        this.isRatify = isRatify;
        this.info = info;
    }

    public boolean isRatify() {
        return isRatify;
    }

    public void setRatify(boolean isRatify) {
        this.isRatify = isRatify;
    }

    public String getReason() {
        return info;
    }

    public void setReason(String info) {
        this.info = info;
    }

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this) ;
    }
}

