package com.soap.ct.writable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by soap on 2018/3/23.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DateDimensionWritable extends BaseDimensionWritable {
    private String year;
    private String month;
    private String day;

    @Override
    public int compareTo(BaseDimensionWritable baseDimensionWritable) {
        DateDimensionWritable anther = (DateDimensionWritable) baseDimensionWritable;
        int result = this.year.compareTo(anther.getYear());
        if (result == 0) {
            result = this.month.compareTo(anther.getMonth());
        }
        if (result == 0) {
            result = this.day.compareTo(anther.getDay());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.year);
        dataOutput.writeUTF(this.month);
        dataOutput.writeUTF(this.day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readUTF();
        this.month = dataInput.readUTF();
        this.day = dataInput.readUTF();
    }
}
