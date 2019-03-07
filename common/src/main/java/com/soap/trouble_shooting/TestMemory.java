package com.soap.trouble_shooting;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author yangfuzhao on 2019/2/26.
 *
 */
public class TestMemory {

    /**
     * 启动 java -cp /Users/soapy/IdeaProjects/big_data/common/target/common-1.0-SNAPSHOT.jar com.soap.trouble_shooting.TestMemory
     * @param args
     */
    public static void main(String[] args) {

        List list = new ArrayList<>();

        while (true){
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            list.add(new Date());
        }

    }
}
