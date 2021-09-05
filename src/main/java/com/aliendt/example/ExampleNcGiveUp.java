package com.aliendt.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

//迟到流重新发送
public class ExampleNcGiveUp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop202",9999);
        SingleOutputStreamOperator<String> result = streamSource
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] strings = value.split(" ");
                        return Tuple2.of(strings[0], Long.parseLong(strings[1])*1000L);
                    }
                })//map之后算子的顺序是如何的呢
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })

                )
                //水位线怎么记:开头ass(waterstrage
                .keyBy(r -> r.f0)
                //马上keyby为什么,因为要开窗分析,
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                //window后面跟着allowedLateness()
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("1234") {
                })
                //这个是什么时候触发,-->window判断延长期之后进入的数据为废弃数据,启动这个算子发送到测流
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        //维护一个状态变量,我没有在写,我在抄
                        ValueState<Boolean> v23456;
                        v23456 = context.windowState().getState(
                                new ValueStateDescriptor<Boolean>("23456", Types.BOOLEAN)
                        );
                        System.out.println(v23456.value());
                        if (v23456.value()==null ) {
                            out.collect("正常进入" + elements.spliterator().getExactSizeIfKnown());

                            v23456.update(true);
                            //状态变量更新用update
                        }else {
                            //有状态怎么办,说明之前已经触发过一次了
                            out.collect("迟到的数据触发," + elements.spliterator().getExactSizeIfKnown() + "个");
                        }

                    }
                });
        result.print("好孩子");
        result.getSideOutput(new OutputTag<Tuple2<String, Long>>("1234"){}).print("不要的孩子");
        env.execute();


    }
}
