package com.aliendt.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Excample6KeyedProcessFunctionOntimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop202", 9999);
        streamSource
                .keyBy(r->true)
                .process(new KeyedProcessFunction<Boolean, String, String>() {
                    //K;I;O是什么意思
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        //用process之前要进行keyby
                        //要不不然类型会报错,然后接下来该怎么办--看泛型,首先是上下文,说了timeservice很重要
                        long time = ctx.timerService().currentProcessingTime();
                        out.collect("数据来了：" + value + ", 到达的时间戳是：" + new Timestamp(time));
                        //接下来该干什么,我想要干什么----设定时间,发射给下游
                        //定时还是上下文对象的活儿
                        ctx.timerService().registerProcessingTimeTimer(time+20*1000L);
                        //此项无法接受变量
                        out.collect("定时时间设定在"+time+20*1000L+"害怕不害怕");
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        //其实这个代码看源码的时候这个方法不是抽象方法,你想要重写他就和toString一样,利用快捷键就出来了
                        out.collect("定时器触发了，触发时间是：" + new Timestamp(timestamp));
                        //重写他的目的就是利用timestamp来toString
                    }
                })
                .print();
        env.execute();

    }
}
