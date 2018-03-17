package com.soap.big_data.hbase.hbase_to_hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/16.
 */
public class FruitMapper extends TableMapper<ImmutableBytesWritable,Put> {



    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        Put put = new Put(key.get());
        for (Cell cell : value.rawCells()){
           if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
               if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                   put.add(cell);
                   System.out.println("Cell : " +Bytes.toString(CellUtil.cloneQualifier(cell)));
               }
           }
        }
       context.write(key,put);
    }
}
