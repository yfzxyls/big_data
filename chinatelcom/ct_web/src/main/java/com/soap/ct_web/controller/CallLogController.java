package com.soap.ct_web.controller;

import com.soap.ct_web.req.QueryCallLogReq;
import com.soap.ct_web.server.CallLogServer;
import com.soap.ct_web.vo.CallLogVo;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.annotation.Resource;
import java.util.Iterator;
import java.util.List;

/**
 * Created by soap on 2018/3/26.
 */
@Controller
public class CallLogController {

    @Resource
    private CallLogServer callLogServer;

    @RequestMapping("/query_call_log")
    private String queryCallLog(Model model, QueryCallLogReq queryCallLogReq) {
        List<CallLogVo> callLogVos = callLogServer.queryCallLogList(queryCallLogReq);
        model.addAttribute("telephone", callLogVos.get(0).getTelephone());
        model.addAttribute("name", callLogVos.get(0).getName());
        model.addAttribute("year", callLogVos.get(0).getYear());
        StringBuilder month = new StringBuilder();
        StringBuilder count = new StringBuilder();
        StringBuilder duration = new StringBuilder();
        for (Iterator<CallLogVo> iterator = callLogVos.iterator(); iterator.hasNext(); ) {
            CallLogVo next =  iterator.next();
            month.append(next.getMonth()).append(",");
            count.append(next.getCall_sum()).append(",");
            duration.append(Float.valueOf(next.getCall_duration_sum()) / 60f).append(",");
        }
        model.addAttribute("month", month.deleteCharAt(month.length() -1));
        model.addAttribute("count", count.deleteCharAt(count.length() -1));
        model.addAttribute("duration", duration.deleteCharAt(duration.length() -1));

        return "jsp/call_log";
    }

}
