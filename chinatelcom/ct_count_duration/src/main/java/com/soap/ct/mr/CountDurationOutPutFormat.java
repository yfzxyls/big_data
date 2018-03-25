package com.soap.ct.mr;

import com.soap.ct.utils.ConnectionSingleton;
import com.soap.ct.utils.SQLUtil;
import com.soap.ct.writable.CommonDimension;
import com.soap.ct.writable.CountValueDimensionWritable;
import lombok.NoArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;

/**
 * Created by soap on 2018/3/23.
 */
public class CountDurationOutPutFormat extends OutputFormat<CommonDimension, CountValueDimensionWritable> {

    private Logger logger = LoggerFactory.getLogger(CountDurationOutPutFormat.class);

    @Override
    public RecordWriter<CommonDimension, CountValueDimensionWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Connection connection = ConnectionSingleton.getInstance();
        return new MySqlRecordWriter(connection);
    }

    @NoArgsConstructor
    protected class MySqlRecordWriter extends RecordWriter<CommonDimension, CountValueDimensionWritable> {
        private Connection connection;

        public MySqlRecordWriter(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void write(CommonDimension commonDimension, CountValueDimensionWritable value) throws IOException, InterruptedException {
            String telPhone = commonDimension.getContactDimensionWritable().getTelephone();
            String name = commonDimension.getContactDimensionWritable().getName();
            String year = commonDimension.getDateDimensionWritable().getYear();
            String month = commonDimension.getDateDimensionWritable().getMonth();
            String day = commonDimension.getDateDimensionWritable().getDay();
            int contactId = SQLUtil.insertContact(telPhone, name, connection);
            int dateId = SQLUtil.insertDateDimension(year, month, day, connection);
            int sum = value.getSum();
            int duration = value.getDuration();
            logger.info(String.format("########## telPhone:%s , name:%s ,year:%s ,month:%s ,day:%s ,contactId;%s ,dateId:%d ,sum:%d ,duration:%d",
                    telPhone, name, year, month, day, contactId, dateId, sum, duration));
            SQLUtil.insertCall(String.valueOf(dateId), String.valueOf(contactId), String.valueOf(sum), String.valueOf(duration), connection);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        }
    }

    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {


    }

    private FileOutputCommitter committer = null;

    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }


}
