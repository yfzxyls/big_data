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
public class ContactDimensionWritable extends BaseDimensionWritable {
    private String telephone;
    private String name;

    @Override
    public int compareTo(BaseDimensionWritable baseDimensionWritable) {
        ContactDimensionWritable anther = (ContactDimensionWritable) baseDimensionWritable;
        int result = this.name.compareTo(anther.getName());
        if (result == 0) {
            result = this.telephone.compareTo(anther.getTelephone());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.telephone);
        dataOutput.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.telephone = dataInput.readUTF();
        this.name = dataInput.readUTF();
    }
}
