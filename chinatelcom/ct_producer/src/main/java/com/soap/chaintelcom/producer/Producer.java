package com.soap.chaintelcom.producer;

import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yangf on 2018/3/20.
 */
public class Producer {

    private List<String> callPhone = new ArrayList<String>();

    private Map<String, String> callName = new HashMap<>();

    public void init() {
        callPhone.add("19254575411");
        callPhone.add("19775072523");
        callPhone.add("17849551445");
        callPhone.add("18193741308");
        callPhone.add("16595140749");
        callPhone.add("16432281360");
        callPhone.add("15650816015");
        callPhone.add("14877108872");
        callPhone.add("16136604361");
        callPhone.add("13668461153");
        callPhone.add("18976970864");
        callPhone.add("13416648848");
        callPhone.add("19258864757");
        callPhone.add("13086622549");
        callPhone.add("14835396873");
        callPhone.add("16689496659");
        callPhone.add("19439689848");
        callPhone.add("14837599278");
        callPhone.add("14203108193");
        callPhone.add("19195971468");
        callPhone.add("15816732758");
        callPhone.add("13143236448");
        callPhone.add("18459741091");
        callPhone.add("19403013723");
        callPhone.add("17491668946");

        callName.put("19254575411", "李雁");
        callName.put("19775072523", "卫艺");
        callName.put("17849551445", "仰莉");
        callName.put("18193741308", "陶欣悦");
        callName.put("16595140749", "施梅梅");
        callName.put("16432281360", "金虹霖");
        callName.put("15650816015", "魏明艳");
        callName.put("14877108872", "华贞");
        callName.put("16136604361", "华啟倩");
        callName.put("13668461153", "仲采绿");
        callName.put("18976970864", "卫丹");
        callName.put("13416648848", "戚丽红");
        callName.put("19258864757", "何翠柔");
        callName.put("13086622549", "钱溶艳");
        callName.put("14835396873", "钱琳");
        callName.put("16689496659", "缪静欣");
        callName.put("19439689848", "焦秋菊");
        callName.put("14837599278", "吕访琴");
        callName.put("14203108193", "沈丹");
        callName.put("19195971468", "褚美丽");
        callName.put("15816732758", "孙怡");
        callName.put("13143236448", "许婵");
        callName.put("18459741091", "曹红恋");
        callName.put("19403013723", "吕柔");
        callName.put("17491668946", "冯怜云");
    }


    /**
     * callfrom , callto , buildtime,duration
     *
     * @return
     */
    public String producer() {
        String callfrom = callPhone.get((int) (Math.random() * callPhone.size()));
        String callto = null;
        while (true) {
            callto = callPhone.get((int) (Math.random() * callPhone.size()));
            if (!callfrom.equals(callto)) break;
        }

        String buildtime = buildTime();

        int duration = (int) (Math.random() * 30 * 60) + 1;
        DecimalFormat decimalFormat = new DecimalFormat("0000");
        String durationStr = decimalFormat.format(duration);
        StringBuilder sb = new StringBuilder();
        sb.append(callfrom).append(",").append(callto)
                .append(",").append(buildtime).append(",").append(durationStr).append("\n");

        return sb.toString();

    }

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    String beginDate = "2017-01-01";
    String endDate = "2018-01-01";

    private String buildTime() {
        return dateFormat.format(Producer.randomDate(beginDate, endDate));
    }

    private void writeLog(String log, OutputStream outputStream){
        try {
            OutputStreamWriter osw = new OutputStreamWriter(outputStream,"UTF-8");
            osw.write(log);
            osw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static Date randomDate(String beginDate, String endDate) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date start = format.parse(beginDate);
            Date end = format.parse(endDate);

            if (start.getTime() >= end.getTime()) {
                return null;
            }

            long date = random(start.getTime(), end.getTime());

            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }


    public static void main(String[] args) throws FileNotFoundException {
        if (args == null || args.length < 1 ){
            System.out.println("参数不合法！");
            return;
        }

        Producer producer = new Producer();
        producer.init();
        OutputStream  outputStream = new FileOutputStream(args[0]);
        while (true){
            String call = producer.producer();
            // System.out.print(call);
            producer.writeLog(call,outputStream);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
