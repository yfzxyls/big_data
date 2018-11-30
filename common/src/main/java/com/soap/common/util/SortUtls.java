package com.soap.common.util;

import java.text.CollationKey;
import java.text.Collator;

/**
 * Created by yangfuzhao on 2018/9/14.
 */
public class SortUtls {

    /**
     * 字符类型按照字典排序
     * @param obj1
     * @param obj2
     * @param isReverse
     * @return
     */
    public static int genComparator(Object obj1, Object obj2, boolean isReverse) {
        //为 null 则排在最后
        if (obj1 == null && obj2 == null) return 0;
        if (obj1 == null) return 1;
        if (obj2 == null) return -1;
        if (isReverse) {
            //字符串按字典排序
            if (obj1 instanceof String) {
                Collator collator = Collator.getInstance(java.util.Locale.CHINESE);
                CollationKey key1 = collator.getCollationKey((String) obj1);
                CollationKey key2 = collator.getCollationKey((String) obj2);
                return key1.compareTo(key2);
            }
            if (obj1 instanceof Comparable) {
                return ((Comparable) obj2).compareTo(obj1);
            }
        }
        return genComparator(obj2, obj1, !isReverse);
    }
}
