package com.soap.ct.mr;

import com.soap.ct.writable.CommonDimension;
import com.soap.ct.writable.CountValueDimensionWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by soap on 2018/3/23.
 */
public class CountDurationReducer extends Reducer<CommonDimension, Text, CommonDimension, CountValueDimensionWritable> {
    private CountValueDimensionWritable countValueDimensionWritable = new CountValueDimensionWritable();

    @Override
    protected void reduce(CommonDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        int duration = 0;
        for (Text text : values) {
            count++;
            duration += Integer.valueOf(text.toString());
        }
        countValueDimensionWritable.setSum(count);
        countValueDimensionWritable.setDuration(duration);
        context.write(key, countValueDimensionWritable);
    }
}
