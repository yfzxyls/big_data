package com.soap.web.controller;

import com.alibaba.fastjson.JSON;
import com.soap.common_behavior.ErrorReportLogs;
import com.soap.common_behavior.PageVisitReportLogs;
import com.soap.common_behavior.StartupReportLogs;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

/**
 */
@Controller()
@RequestMapping("/logs")
public class LogCollector {
    /**
     * 地理信息缓存
     */
    private static final Logger logger = Logger.getLogger(LogCollector.class);

    @PostMapping(value = "/startupLogs")
    @ResponseBody
    public StartupReportLogs startupCollect(@RequestBody StartupReportLogs e, HttpServletRequest req) {

        String LogString = JSON.toJSONString(e);

        // 写入日志目录
        logger.info(LogString);

        return e;
    }

    @RequestMapping(value = "/pageLogs", method = RequestMethod.POST)
    @ResponseBody
    public PageVisitReportLogs pageCollect(@RequestBody PageVisitReportLogs e, HttpServletRequest req) {

        String LogString = JSON.toJSONString(e);

        // 写入日志目录
        logger.info(LogString);

        return e;
    }

    @RequestMapping(value = "/errorLogs", method = RequestMethod.POST)
    @ResponseBody
    public ErrorReportLogs errorCollect(@RequestBody ErrorReportLogs e, HttpServletRequest req) {

        String LogString = JSON.toJSONString(e);

        // 写入日志目录
        logger.info(LogString);

        return e;
    }
}