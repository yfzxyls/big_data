package com.soap.common.thread;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;
import java.util.stream.LongStream;

public class CountTaskTest extends RecursiveTask<Integer> {

    private static final int THREAD_HOLD = 50;

    private int start;
    private int end;

    public CountTaskTest(int start, int end) {
        this.start = start;
        this.end = end;
    }

    @Override
    protected Integer compute() {
        int sum = 0;
        //如果任务足够小就计算
        boolean canCompute = (end - start) <= THREAD_HOLD;
        if (canCompute) {
            for (int i = start; i <= end; i++) {
                sum += i;
                System.out.println(Thread.currentThread().getName());
            }
        } else {
            int middle = (start + end) / 2;
            CountTaskTest left = new CountTaskTest(start, middle); //1 2
            CountTaskTest right = new CountTaskTest(middle + 1, end);// 3 4
            //执行子任务
            left.fork();
            right.fork();
            //获取子任务结果
            int lResult = left.join();
            int rResult = right.join();
            sum = lResult + rResult;
        }
        return sum;
    }

    public static void main(String[] args) {
        ForkJoinPool pool = new ForkJoinPool();
        CountTaskTest task = new CountTaskTest(1, 100);
        Future<Integer> result = pool.submit(task);
        try {
            task.getException();
            task.getRawResult();
            System.out.println(result.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


        long reduce = LongStream.rangeClosed(0, 10000000000L).parallel().reduce(0, Long::sum);

        System.out.println(reduce);
    }
}