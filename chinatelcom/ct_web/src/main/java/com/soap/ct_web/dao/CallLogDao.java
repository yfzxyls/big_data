package com.soap.ct_web.dao;

import com.soap.ct_web.vo.CallLogVo;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * Created by soap on 2018/3/26.
 */
@Repository
public class CallLogDao {

    @Resource
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public List<CallLogVo> queryCallLogList(Map<String, String> queryCallLog) {
        //全年12个月
        String sql = "SELECT t3.id_date_contact, t3.id_date_dimension, t3.id_contact, t3.call_sum, t3.call_duration_sum, t3.telephone, t3.`name`, t4.`year`, t4.`month`, t4.`day` FROM" +
                "( SELECT t2.id_date_contact, t2.id_date_dimension, t2.id_contact, t2.call_sum, t2.call_duration_sum, t1.telephone, t1.`name` FROM" +
                " ( SELECT id, telephone, `name` FROM tb_contacts WHERE telephone = :telephone) t1 JOIN tb_call t2 ON t1.id = t2.id_contact ) t3" +
                " INNER JOIN ( SELECT id, `year`, `month`, `day` FROM tb_dimension_date WHERE `year` = :year AND `month` != :month and `day` = :day ) t4 ON t3.id_date_dimension = t4.id ORDER BY t4.`year`, t4.`month`, t4.`day`;";
        BeanPropertyRowMapper<CallLogVo> contactBeanPropertyRowMapper = new BeanPropertyRowMapper<>(CallLogVo.class);
        List<CallLogVo> callLogVos = namedParameterJdbcTemplate.query(sql, queryCallLog, contactBeanPropertyRowMapper);
        return callLogVos;
    }


}
