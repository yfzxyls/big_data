package com.soap.storm.trident.diagonsis;

import java.io.Serializable;

/**
 * @author yangfuzhao on 2018/12/25.
 */
public class DiagnosisEvent implements Serializable {
    private static final long serialVersionUID = -262574590233216954L;
    public double lat;
    public double lng;
    public long time;
    public String diagnosisCode;

    public DiagnosisEvent(double lat, double lng, long time, String diagnosisCode) {
        super();
        this.time = time;
        this.lat = lat;
        this.lng = lng;
        this.diagnosisCode = diagnosisCode;
    }

    public DiagnosisEvent() {
    }
}
