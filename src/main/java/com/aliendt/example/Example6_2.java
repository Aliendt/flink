package com.aliendt.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example6_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .socketTextStream("hadoop202",9999)
                .keyBy(r->true)
                .process(new KeyedProcessFunction<Boolean, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        long time = ctx.timerService().currentProcessingTime();
                        out.collect(value+" 数据到达的时间为: "+new Timestamp(time));
                        ctx.timerService().registerProcessingTimeTimer(time+20*1000L);
                        out.collect("我们将时间设定在了: "+new Timestamp(time)+20*1000L);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect(" 触发器触发了,时间将在: "+new Timestamp(timestamp));
                    }
                })
                .print();
        env.execute();
    }
}
