package com.soap.common.stream;

import com.google.common.collect.Lists;
import com.soap.common.util.SortUtls;
import org.junit.Test;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

public class StreamApi {

    @Test
    public void listStream() {

        List<Integer> integers = Lists.newArrayList(1, 2, 3, 3, 4, 5, 6, 7, 8, 9);

        Stream<Integer> stream = integers.stream();
        stream.filter(i -> i > 5).forEach(System.out::print);
        System.out.println();

        //一个stream 对象只能使用一次
        //distinct
        integers.stream().distinct().forEach(System.out::print);

        //limit 取前n个（包括n）
        System.out.println("\nlimit");
        integers.stream().limit(3).forEach(System.out::print);

        //skip 取第n个以后(不包括n)
        System.out.println("\nskip");
        integers.stream().skip(3).forEach(System.out::print);

        //map 映射操作
        System.out.println("\nmap");
        integers.stream().map(i -> i * 2).forEach(System.out::print);

        //map 映射操作
        System.out.println("\nmap");
        integers.stream().mapToDouble(i -> i * 2).forEach(System.out::print);


        ArrayList<String> strings = Lists.newArrayList("qwe", "asd", "zxc");

        strings.stream().flatMap(i -> {
            List<Character> list = new ArrayList<>();
            char[] chars = i.toCharArray();
            for (int j = 0; j < chars.length; j++) {
                char aChar = chars[j];
                list.add(aChar);
            }
            return list.stream();
        });

    }


    @Test
    public void sort() {

        List list = Lists.newArrayList("你", "你", "吃", "饭", "了", "吗");

        System.out.println();
        list.stream().sorted().forEach(System.out::print);

        System.out.println();

        list.sort((v1, v2) -> SortUtls.genComparator(v1, v2, true));
        list.forEach(System.out::print);

    }

    @Test
    public void stat() {
        List<Integer> integers = Lists.newArrayList(1, 2, 3, 3, 4, 5, 6, 7, 8, 9);
        DoubleSummaryStatistics statistics = integers.stream().mapToDouble(x -> (double) x).summaryStatistics();
        System.out.println("平均值：" + statistics.getAverage());
        System.out.println("最大值：" + statistics.getMax());
        System.out.println("最小值：" + statistics.getMin());
        System.out.println("数据量：" + statistics.getCount());
        System.out.println("和：" + statistics.getSum());

        double asDouble = integers.stream().mapToInt(x -> x).average().getAsDouble();
        System.out.println(asDouble);

    }

    @Test
    public void random(){
        Random random = new Random();
        double asDouble = random.ints(100).average().getAsDouble();

        System.out.println(asDouble);

    }

    @Test
    public void time(){
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime localDateTime = now.plusDays(1);
        System.out.println(localDateTime.toString());
    }


}
