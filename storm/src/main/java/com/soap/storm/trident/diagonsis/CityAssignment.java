package com.soap.storm.trident.diagonsis;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangfuzhao on 2018/12/25.
 * BaseFunction 与 bolt的功能类似，区别在于BaseFunction 只能添加字段，不能删除和变更字段
 *
 *
 */
public class CityAssignment extends BaseFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CityAssignment.class);

    private static Map<String, double[]> CITIES = new HashMap<String, double[]>();

    /**
     * 初始化城市坐标
     */
    {
        double[] phl = { 39.875365, -75.249524 };
        CITIES.put("PHL", phl);
        double[] nyc = { 40.71448, -74.00598 };
        CITIES.put("NYC", nyc);
        double[] sf = { -31.4250142, -62.0841809 };
        CITIES.put("SF", sf);
        double[] la = { -34.05374, -118.24307 };
        CITIES.put("LA", la);
    }

    /**
     * 根据距离信息增加城市
     * @param tuple
     * @param collector
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
        double leastDistance = Double.MAX_VALUE;
        String closestCity = "NONE";
        for (Map.Entry<String, double[]> city : CITIES.entrySet()) {
            double R = 6371; // km
            double x = (city.getValue()[0] - diagnosis.lng) * Math.cos((city.getValue()[0] + diagnosis.lng) / 2);
            double y = (city.getValue()[1] - diagnosis.lat);
            double d = Math.sqrt(x * x + y * y) * R;
            if (d < leastDistance) {
                leastDistance = d;
                closestCity = city.getKey();
            }
        }
        List<Object> values = new ArrayList<Object>();
        values.add(closestCity);
        LOG.debug("Closest city to lat=[" + diagnosis.lat + "], lng=[" + diagnosis.lng + "] == [" + closestCity
                + "], d=[" + leastDistance + "]");
        collector.emit(values);
    }
}
