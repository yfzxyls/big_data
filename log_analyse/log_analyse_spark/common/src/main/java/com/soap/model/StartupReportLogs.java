package com.soap.model;

import lombok.Data;

/**
 * 启动日志
 */
@Data
public class StartupReportLogs extends BasicLog {
    private static final long serialVersionUID = 123456789L;
    private String appVersion;
    private Long startTimeInMs;
    private Long activeTimeInMs;
    private String city;
    private String userId;

}
