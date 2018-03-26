package com.soap.ct_web.server;

import com.soap.ct_web.dao.CallLogDao;
import com.soap.ct_web.req.QueryCallLogReq;
import com.soap.ct_web.vo.CallLogVo;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by soap on 2018/3/26.
 */
@Service
public class CallLogServer {

    @Resource
    private CallLogDao callLogDao;

    public List<CallLogVo> queryCallLogList(QueryCallLogReq queryCallLogReq) {
        Map<String, String> queryCall = new HashMap<>();
        queryCall.put("telephone",queryCallLogReq.getTelephone());
        queryCall.put("year",queryCallLogReq.getYear());
        queryCall.put("month",queryCallLogReq.getMonth());
        queryCall.put("day",queryCallLogReq.getDay());
       return callLogDao.queryCallLogList(queryCall) ;
    }
}
