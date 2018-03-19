package com.soap.weibo.utils;

import org.joda.time.DateTime;

/**
 * Created by yangf on 2018/3/18.
 */
public class DateUtil {

    public static void main(String[] args) {
        long str = System.currentTimeMillis();
        String datestr = String.valueOf(str);
        String date = new DateTime(datestr).toString();
        System.out.println(date);
    }
}
