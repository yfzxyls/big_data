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
public class CountValueDimensionWritable extends BaseValueWritable {

    private int sum;
    private int duration;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(sum);
        dataOutput.writeInt(duration);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
         this.sum = dataInput.readInt();
         this.duration = dataInput.readInt();
    }
}
