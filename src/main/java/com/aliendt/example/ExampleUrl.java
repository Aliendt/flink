package com.aliendt.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

// 增量聚合函数的使用
public class ExampleUrl {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new CustomSource())
                .keyBy(r->r.user)
                //为什么要keyby---利用user来进行分类,逻辑分区,控制输入数据的类型
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //开窗之后如何实现内部逻辑----点window先指定类型"TumblingEventTimeWindows",这个类型不对,
                // 应该是(TumblingProcessingTimeWindows)这个是滚动窗口
                // 增量聚合函数 √
                //全窗口聚合函数
                .aggregate(new CountAgg() )
                //其数据来源是Event所以泛型怎么定---自己说了来源是Event,那输入的泛型就是Event
                .print("====增量聚合函数=======");
                //意思是将上一个算子的结构传入下一个算子
        env
                .addSource(new CustomSource())
                .keyBy(r->r.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //window这个方法是干啥的?
                .aggregate(new CountAgg(),new WindGg())
                .print("==增量聚合函数=全窗口聚合函数=");
        env.execute();
    }
    public static class CustomSource implements ParallelSourceFunction<Event> {
        private boolean running = true;
        private Random random = new Random();
        private String[] userArr = {"Mary", "Bob", "Alice", "Liz"};
        private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                // 使用collect方法输出数据
                ctx.collect(
                        new Event(
                                userArr[random.nextInt(userArr.length)],
                                urlArr[random.nextInt(urlArr.length)],
                                Calendar.getInstance().getTimeInMillis()
                        )
                );
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Event {
        public String user;
        public String url;
        public Long timestamp;

        public Event() {
        }

        public Event(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
    public static class CountAgg implements AggregateFunction<Event,Long,Long>{
        //in ;acc;out
        @Override
        public Long createAccumulator() {
            //这个就是累加器的初始化,先给一个0
            //spark中的累加器好像还有清空clear
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            //累加器的内部逻辑,之前spark会声明一个公共的list集合
            return accumulator+1L;
        }

        @Override
        public Long getResult(Long accumulator) {

            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
        //spark中的累加器的泛型有输入和输出,还有累加器类型


        }
    /*public static class ConGg2 implements AggregateFunction<Event,Long,Long>{
        //本实现类的输出类型为Long,我为什么要写这个,因为他要作为下一个类型的输入来运行
        @Override
        public Long createAccumulator() {
            return null;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return null;
        }

        @Override
        public Long getResult(Long accumulator) {
            return null;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }*/
    public static class WindGg extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
        //他的输入和输出in out key w,你瞅瞅这个细节,ProcessWindowFunction就变成了Extends,
        // 打印出什么东西来应该看的是掉调用方法的输出是什么
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            //输出逻辑如何记忆?看本方法传入参数:
            // s 为key;
            // context为上下文;
            // Long型迭代器为输入;
            // UserViewCountPerWindow的接收器为输出,他其中套着Event
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long count = elements.iterator().next();
            out.collect(new UserViewCountPerWindow( s,count,start,end));
            //增量聚合无法获得窗口信息:user;count;start;end;


        }

    }
    public static class UserViewCountPerWindow {
        public String user;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public UserViewCountPerWindow() {
        }

        public UserViewCountPerWindow(String user, Long count, Long windowStart, Long windowEnd) {
            this.user = user;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "UserViewCountPerWindow{" +
                    "user='" + user + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }
    //ProcessWindowFunction所用的实现类

}
