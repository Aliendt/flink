package com.aliendt.example;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

// 实时热门商品
// 每隔5分钟，统计过去一个小时的最热门的商品
// 对于离线数据集来说，flink只会插入两次水位线，开头插入负无穷大，结尾插入正无穷大
public class Example8shopping {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

       env
                .readTextFile("D:\\java\\Cade\\flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(r -> r.windowEnd)
        //pv.print("===========开窗之后的输出============");

                .process(new TopN(3))
                .print("======最后的输出=======");

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private int n;

        public TopN(int n) {
            this.n = n;
        }

        // 初始化一个列表状态变量，用来保存ItemViewCount
        private ListState<ItemViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("list-state", Types.POJO(ItemViewCount.class))
            );
            //保证单例
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一个ItemViewCount就存入列表状态变量
            listState.add(value);
            // 不会重复注册定时器
            // 定时器用来排序，因为可以确定所有ItemViewCount都到了
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1L);
            //转移变量;设定定时器
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 将数据取出排序
            ArrayList<ItemViewCount> itemViewCounts = new ArrayList<>();
            for (ItemViewCount ivc : listState.get()) {
                itemViewCounts.add(ivc);
            }
            listState.clear();
            //交接成功以后开始动itemViewCount

            // 按照浏览量降序排列
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount t2, ItemViewCount t1) {
                    return t1.count.intValue() - t2.count.intValue();
                }
            });
            //升序

            StringBuilder result = new StringBuilder();
            result
                    //String类型的方法,result为自定义的字符串类型,利用方法拼接我们想要的内容
                    .append("==================================================\n")
                    .append("窗口结束时间：" + new Timestamp(timestamp - 1L))
                    .append("\n");

            for (int i = 0; i < n; i++) {
                ItemViewCount currIvc = itemViewCounts.get(i);
                result
                        .append("第" + (i+1) + "名的商品ID是：" + currIvc.itemId + "，浏览量是：" + currIvc.count + "\n");
            }
            result
                    .append("==================================================\n");
            out.collect(result.toString());
        }
    }
    //绝了~

    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        //in上一个类传入的聚合数值;
        // out:给窗口单定义的一个包装类
        // key;上文keyby确定的属性
        // wind:时间窗口
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(
                    s,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        //in ;acc; out
        //在这个类中我没有看到key--Itemid的身影,应该是按itemid分类出现一次计数一次
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
            //这个类的逻辑是个什么意思,只要是来就加一
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class ItemViewCount {
        public String itemId;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public ItemViewCount() {
        }

        public ItemViewCount(String itemId, Long count, Long windowStart, Long windowEnd) {
            this.itemId = itemId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemViewCount{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }

    public static class UserBehavior {
        public String userId;
        public String itemId;
        public String categoryId;
        public String behavior;
        public Long timestamp;

        public UserBehavior() {
        }

        public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
