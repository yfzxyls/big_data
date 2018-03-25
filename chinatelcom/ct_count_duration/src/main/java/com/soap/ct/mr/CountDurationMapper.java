package com.soap.ct.mr;

import com.soap.ct.writable.CommonDimension;
import com.soap.ct.writable.ContactDimensionWritable;
import com.soap.ct.writable.DateDimensionWritable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by soap on 2018/3/23.
 */
public class CountDurationMapper extends TableMapper<CommonDimension, Text> {

    private Map<String, String> callName = null;

    private Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        callName = new HashMap<>();
        callName.put("19254575411", "李雁");
        callName.put("19775072523", "卫艺");
        callName.put("17849551445", "仰莉");
        callName.put("18193741308", "陶欣悦");
        callName.put("16595140749", "施梅梅");
        callName.put("16432281360", "金虹霖");
        callName.put("15650816015", "魏明艳");
        callName.put("14877108872", "华贞");
        callName.put("16136604361", "华啟倩");
        callName.put("13668461153", "仲采绿");
        callName.put("18976970864", "卫丹");
        callName.put("13416648848", "戚丽红");
        callName.put("19258864757", "何翠柔");
        callName.put("13086622549", "钱溶艳");
        callName.put("14835396873", "钱琳");
        callName.put("16689496659", "缪静欣");
        callName.put("19439689848", "焦秋菊");
        callName.put("14837599278", "吕访琴");
        callName.put("14203108193", "沈丹");
        callName.put("19195971468", "褚美丽");
        callName.put("15816732758", "孙怡");
        callName.put("13143236448", "许婵");
        callName.put("18459741091", "曹红恋");
        callName.put("19403013723", "吕柔");
        callName.put("17491668946", "冯怜云");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 01_16379727058_20191120150958_14607885120_1_1178
        String[] fields = Bytes.toString(value.getRow()).split("_");
        String flag = fields[4];
        if ("0".equals(flag)) return;
        String caller = fields[1];
        String callee = fields[3];
        String buildTime = fields[2];
        String duration = fields[5];

        String year = buildTime.substring(0, 4);
        String month = buildTime.substring(4, 6);
        String day = buildTime.substring(6, 8);
        outValue.set(duration);

        //主叫年
        writeOut(context, caller, year, "-1", "-1");
        //主叫月
        writeOut(context, caller, year, month, "-1");
        //主叫天
        writeOut(context, caller, year, month, day);
        
        //被叫年
        writeOut(context, callee, year, "-1", "-1");
        //被叫月
        writeOut(context, callee, year, month, "-1");
        //被叫天
        writeOut(context, callee, year, month, day);

    }

    ContactDimensionWritable contactDimensionWritable = new ContactDimensionWritable();
    DateDimensionWritable dateDimension = new DateDimensionWritable();
    CommonDimension commonDimension = new CommonDimension();
    private void writeOut(Context context, String telPhone, String year, String month, String day) throws IOException, InterruptedException {
        contactDimensionWritable.setTelephone(telPhone);
        contactDimensionWritable.setName(callName.get(telPhone));
        dateDimension.setYear(year);
        dateDimension.setMonth(month);
        dateDimension.setDay(day);
        commonDimension.setContactDimensionWritable(contactDimensionWritable);
        commonDimension.setDateDimensionWritable(dateDimension);
        context.write(commonDimension, outValue);
    }
}
