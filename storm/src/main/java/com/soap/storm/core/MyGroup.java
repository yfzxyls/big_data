package com.soap.storm.core;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.List;

/**
 * @author yangfuzhao on 2018/11/30.
 */
public class MyGroup implements CustomStreamGrouping {
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {

    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        return null;
    }
}
