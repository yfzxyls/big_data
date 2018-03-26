package com.soap.ct_web.req;

import lombok.Data;

/**
 * Created by soap on 2018/3/26.
 */
@Data
public class QueryCallLogReq {
    private String telephone;
    private String year;
    private String month;
    private String day;

}
