package com.soap.ct.mr;

import com.soap.ct.convert.DimensionConvertIml;
import com.soap.ct.convert.IConvert;
import com.soap.ct.utils.ConnectionSingleton;
import com.soap.ct.utils.JDBCUtil;
import com.soap.ct.writable.CommonDimension;
import com.soap.ct.writable.CountValueDimensionWritable;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by soap on 2018/3/23.
 */
public class CountDurationOutPutFormat1 extends OutputFormat<CommonDimension, CountValueDimensionWritable> {

    private Logger logger = LoggerFactory.getLogger(CountDurationOutPutFormat1.class);

    @Override
    public RecordWriter<CommonDimension, CountValueDimensionWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Connection connection = ConnectionSingleton.getInstance();
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new MySqlRecordWriter(connection);
    }

    @NoArgsConstructor
    @AllArgsConstructor
    protected class MySqlRecordWriter extends RecordWriter<CommonDimension, CountValueDimensionWritable> {
        private Connection connection = null;
        //mysql 客户端缓存最大值
        private int batchSize;
        //已经缓存数量
        private int batchCount;
        private PreparedStatement preparedStatement = null;
        private IConvert convert = null;

        public MySqlRecordWriter(Connection connection) {
            this.connection = connection;
            this.batchSize = 500;
            this.batchCount = 0;
            this.convert = new DimensionConvertIml();
        }

        @Override
        public void write(CommonDimension commonDimension, CountValueDimensionWritable value) throws IOException, InterruptedException {
            int contactId = convert.getDimensionId(commonDimension.getContactDimensionWritable());
            int dateId = convert.getDimensionId(commonDimension.getDateDimensionWritable());
            int sum = value.getSum();
            int duration = value.getDuration();
            String sql = "INSERT INTO ct.tb_call(id_date_contact, id_date_dimension, id_contact, call_sum, call_duration_sum) VALUES (?,?,?,?,?)" +
                    " ON DUPLICATE KEY UPDATE id_date_contact = ?;";
            try {
                if (preparedStatement == null){
                    preparedStatement = connection.prepareStatement(sql);
                }
              
                preparedStatement.setString(1, contactId + "_" + dateId);
                preparedStatement.setInt(2, dateId);
                preparedStatement.setInt(3, contactId);
                preparedStatement.setInt(4, sum);
                preparedStatement.setInt(5, duration);
                preparedStatement.setString(6, contactId + "_" + dateId);
                preparedStatement.addBatch();
                batchCount++;
                if (batchCount >= batchSize) {
                    preparedStatement.executeBatch();
                    connection.commit();
                    batchCount = 0;
                }
                logger.info("############ contact_data_id : " + contactId + "_" + dateId);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                logger.info("############ close  " );
                preparedStatement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            JDBCUtil.close(null, preparedStatement, null);
        }
    }

    public void checkOutputSpecs(JobContext job) throws IOException {


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
