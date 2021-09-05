package com.aliendt.example;

import org.apache.flink.api.common.functions.AggregateFunction;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Calendar;

public class Example5AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       env
               .addSource(new ExampleUrl.CustomSource())
               .keyBy(r->r.user)
               .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
               .aggregate(new CountGg2())
               .print();
       env.execute();
    }
    public static class CountGg2 implements AggregateFunction<ExampleUrl.Event, Tuple4<String,Long,Long,Long>, ExampleUrl.UserViewCountPerWindow> {
        //本方法之前的泛型:in-->Event,acc-->Long,out--->Long
        //本方法的需求是要获得窗口信息:user;count;start;end
        // 那么in;acc;out分别填什么,
        // 重写哪些方法:初始化;add;get;merge,
        // 所需的数据来自于哪里-按user流过来的Event
        //全窗口聚合函数:上下文对象传递信息,收集器收集窗口信息
        @Override
        public Tuple4<String, Long, Long, Long> createAccumulator() {
            //初始化,注意Tuple4带of
            return Tuple4.of("", 0L, 0L, 0L);
        }

        @Override
        public Tuple4<String, Long, Long, Long> add(ExampleUrl.Event value, Tuple4<String, Long, Long, Long> accumulator) {
            long timeInMillis = Calendar.getInstance().getTimeInMillis();
            Long startime=timeInMillis-timeInMillis%5000L;
            Long endtime=startime+5000L;
            Long count=accumulator.f1+1;
            return Tuple4.of(value.user,count,startime,endtime);
        }

        @Override
        public ExampleUrl.UserViewCountPerWindow getResult(Tuple4<String, Long, Long, Long> accumulator) {
            return new ExampleUrl.UserViewCountPerWindow(
                    accumulator.f0,accumulator.f1,accumulator.f2,accumulator.f3
            );
        }

        @Override
        public Tuple4<String, Long, Long, Long> merge(Tuple4<String, Long, Long, Long> a, Tuple4<String, Long, Long, Long> b) {
            return null;
        }


    }
}
