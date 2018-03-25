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
public class CommonDimension extends BaseDimensionWritable {
    private ContactDimensionWritable contactDimensionWritable = new ContactDimensionWritable();
    private DateDimensionWritable dateDimensionWritable = new DateDimensionWritable();

    @Override
    public int compareTo(BaseDimensionWritable o) {
        CommonDimension anther = (CommonDimension) o;
        int result = this.contactDimensionWritable.compareTo(anther.getContactDimensionWritable());
        if (result == 0) {
            result = this.dateDimensionWritable.compareTo(anther.getDateDimensionWritable());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        contactDimensionWritable.write(dataOutput);
        dateDimensionWritable.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        contactDimensionWritable.readFields(dataInput);
        dateDimensionWritable.readFields(dataInput);
    }
}
