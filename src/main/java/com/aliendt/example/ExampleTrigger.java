package com.aliendt.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ExampleTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ExampleUrl.CustomSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ExampleUrl.Event>forMonotonousTimestamps()
                                //水位线的泛型是什么意思,T是什么意思,
                                // new CustomSource只是生成,其中原数据的类型其实是event
                                .withTimestampAssigner(
                                new SerializableTimestampAssigner<ExampleUrl.Event>() {
                                    @Override
                                    public long extractTimestamp(ExampleUrl.Event element, long recordTimestamp) {
                                        //记住,水位线的泛型是数据类型,如Tuple<>和Event
                                        return element.timestamp;
                                    }
                                }
                        )
                )
                .keyBy(r->true)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                //开窗不是new,也不是all,沙雕
                .trigger(new MyTr())
                .process(new ProcessWindowFunction<ExampleUrl.Event, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, Context context, Iterable<ExampleUrl.Event> elements, Collector<String> out) throws Exception {
                        //此方法泛型起何作用:in;out;key;window
                        long count = elements.spliterator().getExactSizeIfKnown();
                        long windowstart = context.window().getStart();
                        long windowend = context.window().getEnd();
                        out.collect("窗口: "+new Timestamp(windowstart)+"到"+new Timestamp(windowend)+"中有 "+count+"个pv数据");
                    }
                })
                .print();
            env.execute();

    }

    public static class MyTr extends Trigger<ExampleUrl.Event, TimeWindow> {
        //trigger的泛型没有写对，process就会发黄
        // 不管是继承类还是实现接口，泛型确定了才能实现方法
        @Override
        public TriggerResult onElement(ExampleUrl.Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            //接下来的逻辑是什么:
            ValueState<Boolean> tem=ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("1234", Types.BOOLEAN)
                    //这一点一点抄,写个屁,记也记不住
                    //这里为什么是T,不是key
            );//根据上下文获得单例
            Long time1=element.timestamp-element.timestamp%10000L+10000L;

            if (tem.value()==null){
                ctx.registerEventTimeTimer(time1);
                tem.update(true);
            }

            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            //传入的这个time就是onelement方法注册的时间戳(传入第一个数据的时间戳的下一个10秒),你要的是下下个十秒
            if ((time<window.getEnd())){
                ctx.registerEventTimeTimer((time+10000L));
                //小于就继续fire,如果不小于就什么也不做
                return TriggerResult.FIRE;
            }

            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> tem=ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("1234", Types.BOOLEAN)
                    //这一点一点抄,写个屁,记也记不住
                    //这里为什么是T,不是key
            );
            tem.clear();
            //又写完一个,但是记住不啊


        }




    }
}
