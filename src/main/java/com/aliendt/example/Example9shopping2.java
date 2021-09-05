package com.aliendt.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
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

public class Example9shopping2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);
        evn
                .readTextFile("D:\\java\\Cade\\flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, RowSouce>() {
                    @Override
                    public RowSouce map(String value) throws Exception {
                        //为什么我想不到split
                        String[] s = value.split(",");
                        return new RowSouce(
                                s[0],s[1],s[2],s[3],
                                Long.parseLong(s[4])*1000L
                        );
                    }
                })
                .filter(r->r.behavior.equals("pv"))
                //赶快开窗
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<RowSouce>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<RowSouce>() {
                            @Override
                            public long extractTimestamp(RowSouce element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })

                )
                .keyBy(r->r.itemid)
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .aggregate(new CountAgg(),new WindowResult())
                //aggregate之后是崭新的数据源
                .keyBy(r->r.windStop)
                .process(new TopNum(3))
                .print();
        evn.execute();
    }
    public static class RowSouce{
        public String userid;
        public String itemid;
        public  String categoryId;
        public String behavior;
        public Long timestamp;

        public RowSouce() {
        }

        public RowSouce(String userid, String itemid, String categoryId, String behavior, Long timestamp) {
            this.userid = userid;
            this.itemid = itemid;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "RowSouce{" +
                    "userid='" + userid + '\'' +
                    ", itemid='" + itemid + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    public static class CountAgg implements AggregateFunction<RowSouce,Long,Long> {
        //傻蛋,泛了型再重写方法

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(RowSouce value, Long accumulator) {
            return accumulator+1;
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
    public static class WindowResult extends ProcessWindowFunction<Long,UserResult,String , TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UserResult> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long count = elements.iterator().next();
            out.collect(new UserResult(
                    s,count,start,end
            ));

        }
    }
    public static class UserResult{
        public String itemId;
        public Long count;
        public Long windStart;
        public Long windStop;

        public UserResult() {
        }

        public UserResult(String itemId, Long count, Long windStart, Long windStop) {
            this.itemId = itemId;
            this.count = count;
            this.windStart = windStart;
            this.windStop = windStop;
        }

        @Override
        public String toString() {
            return "UserResult{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windStart=" + windStart +
                    ", windStop=" + windStop +
                    '}';
        }
    }
    public static class TopNum extends KeyedProcessFunction<Long,UserResult,String>{
        //好烦呀这玩意儿
        private ListState<UserResult> listState;
        private int n;

        public TopNum(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
      listState= getRuntimeContext().getListState(
               new ListStateDescriptor<UserResult>("list-state", Types.POJO(UserResult.class))
              //这种泛型错误的情况如何避免,我的妈呀
            );
        }
        //本步骤保证单例

        @Override
        public void processElement(UserResult value, Context ctx, Collector<String> out) throws Exception {
            //这一步要干什么?,之前从一个类转到另一个类老师介绍了一种工具类,但是这里能用吗,
            //想多了,他是一个集合而已
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windStop+1L);
             //定时器怎么弄,注册,注册注册,懂不懂,注册
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            //你怎么像,全局变量才能提到外头
            ArrayList<UserResult> arrayList = new ArrayList<>();
            //泛型问题这他么怎么解决,两回了
            for (UserResult k:listState.get()
                 ) {
                arrayList.add(k);
            }
            listState.clear();
            arrayList.sort(new Comparator<UserResult>() {
                @Override
                public int compare(UserResult o1, UserResult o2) {
                    return o2.count.intValue()-o1.count.intValue();
                }
            });

            //接下来怎么取出来
            StringBuilder s = new StringBuilder();
            s
                    .append("===========================\n")
                    .append("窗口结束时间为: "+new Timestamp(timestamp-1L))
                    .append("\n");
            for (int i = 0; i < n; i++) {
                UserResult userResult = arrayList.get(i);
                s
                        .append("第" + (i+1) + "名的商品ID是：" + userResult.itemId + "，浏览量是：" + userResult.count + "\n");
            }
            s
                    .append("==================================================\n");
            out.collect(s.toString());



        }
    }

}
