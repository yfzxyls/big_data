package com.soap.study.youtube_etl;

import org.apache.commons.lang.StringUtils;

/**
 * Created by yangf on 2018/3/9.
 */
public class ETLUtil {

    public static String videoETL(String ori) {
        if (StringUtils.isBlank(ori)) return null;
        String[] split = ori.split("\t");
        if (split == null || split.length < 9) return null;
        split[3] = split[3].replace(" ", "");
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < split.length; i++) {
            sb.append(split[i]);
            if (i < 9) {
                sb.append("\t");
            } else {
                sb.append("&");
            }
        }
        return sb.substring(0,sb.length()-1);
    }

    public static void main(String[] args) {
        String ori = "RX24KLBhwMI\tlemonette\t697\tPeople & Blogs\t512\t24149\t4.22\t315\t474";
        System.out.println(videoETL(ori));
    }
}
