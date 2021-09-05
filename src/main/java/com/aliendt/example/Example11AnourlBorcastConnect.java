package com.aliendt.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Example11AnourlBorcastConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<ExampleUrl.Event> clicksource = env.addSource(new ExampleUrl.CustomSource());
        DataStreamSource<String> quarysource = env.socketTextStream("hadoop202", 9999);

        //调用者
        clicksource
                .keyBy(r->r.user)
                //接下来写什么,另一个变量是端口输入的,要将其并行度设为1
                // 然后利用brocase广播变量,让其他slot也能看到,那么什么时候也见到过广播变量
                // spark中写需求的时候也出现过广播变量,spark的广播变量算子是为了经过shuffle阶段前后的算子,
                // 也可以使用到此变量,好像是Thread线程类干的好事
                .connect(quarysource.setParallelism(1).broadcast())
                //接下来要干什么,防止端口乱输入信息
                //本来是用flatMap的,然后用这个也行
                //老师就说过,keyprocessFunction可以解决map/flatmap.和filter这三个算子的共能
                .process(new CoProcessFunction<ExampleUrl.Event, String, ExampleUrl.Event>() {
                    //维护一个私有的公共字符串:
                    private String query=" ";
                    @Override
                    public void processElement1(ExampleUrl.Event value, Context ctx, Collector<ExampleUrl.Event> out) throws Exception {
                    //逻辑是什么样的
                        if (value.url.equals(query)){
                            out.collect(value);
                        }
                    }

                    @Override
                    public void processElement2(String value, Context ctx, Collector<ExampleUrl.Event> out) throws Exception {
                    query=value;
                    }
                })
                .print();
        env.execute();
        //又整完一个



    }
}
