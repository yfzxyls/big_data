package com.soap.ct_web.vo;

import lombok.Data;

/**
 * Created by soap on 2018/3/26.
 */
@Data
public class CallLogVo {
    private String telephone;
    private String call_sum;
    private String call_duration_sum;
    private String name;
    private String year;
    private String month;
    private String day;
}
