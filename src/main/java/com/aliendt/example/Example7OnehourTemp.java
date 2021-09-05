package com.aliendt.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Random;
//状态变量,时间戳
//什么叫连续1秒温度上升
public class Example7OnehourTemp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                //我要想写一秒钟的温度上升,就不能使用这个数据源
                .addSource(new SensorSource())
                .keyBy(r->r.id)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {
                    //k ; i ;o---key应该是和上一个算子的keyby相对应 i-->in ;o -->out
                    private ValueState<Double> lastTemp;
                    private ValueState<Long> timeTs;
                    //时间用Long,温度用Double

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化
                        super.open(parameters);
                        //赋值,怎么赋值,描述一下:单例
                        lastTemp=getRuntimeContext().getState(
                            new ValueStateDescriptor<Double>("last-temp",Types.DOUBLE)
                        );
                        //一点儿一点儿抄,一点儿一点儿抄
                        timeTs=getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("time-ts",Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
                        //一点儿一点儿抄
                        Double pretemp=null;
                        //之前的温度临时容器,他不丢,他存起来
                        if (lastTemp.value()!=null){
                            //有值
                            pretemp=lastTemp.value();
                        }
                        lastTemp.update(value.temperature);
                        //为什么要这样做,他将原来的值赋予pretemp,然后将新的value重新刷入了lasttemp当中
                        Long ts=null;
                        //ts何时为空,没有值刷他,刚开始的时候
                        if (timeTs.value()!=null){
                            ts=timeTs.value();
                            //时间戳不丢,存起来
                        }
                        if (pretemp==null||value.temperature<pretemp){
                            //如果之前的温度为空,或者传入的温度要小于之前的温度
                            //来过数据---pretemp不为空;没来过---ts清空,为后续做准备
                            //传入数据没有上升,也清空,为下一次做准备
                            if (ts!=null)ctx.timerService().deleteProcessingTimeTimer(ts);
                            //将ts时间戳清空

                        }else if (value.temperature>pretemp&&ts==null){
                            //传入温度大于上一次温度,而且ts时间戳为空,刚开始的时候,上一次让刷掉的时候
                            ctx.timerService().registerProcessingTimeTimer(
                                    ctx.timerService().currentProcessingTime()+1000L
                            );
                            timeTs.update(ctx.timerService().currentProcessingTime()+1000L);
                            //新的变化温度来了,而且比原来的大,记上时间.更新温度数据,
                            // 连续一秒是瞬时速度,为啥,新温度来了,一秒以后,OnTimer方法被触发,打印

                        }

                    }



                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("温度连续1秒钟上升了，传感器id是：" + ctx.getCurrentKey());
                        timeTs.clear();
                    }
                })
                .print();
        env.execute();

    }
    //贴一个SourceFunction实现类:
    //此实现类的逻辑和之前写过的是其他数据源的逻辑是一样的:run方法和cancel
    public static class SensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private Random random = new Random();
        private String[] sensorIds = {
                //打印使用
                "sensor_1",
                "sensor_2",
                "sensor_3",
                "sensor_4",
                "sensor_5",
                "sensor_6",
                "sensor_7",
                "sensor_8",
                "sensor_9",
                "sensor_10"
        };


        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            double[] temps = new double[10];
            for (int i = 0; i < 10; i++) {
                temps[i] = 65 + 20 * random.nextGaussian();
                //生成随机温度
            }
            while (running) {
                long currTs = Calendar.getInstance().getTimeInMillis();
                for (int i = 0; i < 10; i++) {
                    ctx.collect(SensorReading.of(
                            sensorIds[i],
                            temps[i] + random.nextGaussian() * 0.5,
                            currTs
                    ));
                }
                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
    //贴一个POJO类:
    public static class SensorReading {
        public String id;
        public Double temperature;
        public Long timestamp;

        public SensorReading() {
        }

        public SensorReading(String id, Double temperature, Long timestamp) {
            this.id = id;
            this.temperature = temperature;
            this.timestamp = timestamp;
        }

        public static SensorReading of(String id, Double temperature, Long timestamp) {
            return new SensorReading(id, temperature, timestamp);
            //这个是不是快捷new,之前的POJO类怎么没有看到过
        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "id='" + id + '\'' +
                    ", temperature=" + temperature +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

}
