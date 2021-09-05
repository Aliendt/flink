package com.aliendt.example;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Examp10InnerJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //复制一下数据源,这个fromElements在刚开始的时候用过
        //我们现在都用到过哪几个数据源:fromElement(集合数组);addsource(自定义数据源);readTextFile(文件);sockeTextstream(端口);
        DataStreamSource<Tuple2<String, Integer>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("b", 1),
                        Tuple2.of("c", 1)
                );

        DataStreamSource<Tuple2<String, Integer>> stream2 = env
                .fromElements(
                        Tuple2.of("a", 2),
                        Tuple2.of("a", 3),
                        Tuple2.of("b", 2)
                );
        //拿到数据源以后我们该干什么
        stream1.keyBy(r->r.f0)
                .connect(stream2.keyBy(r->r.f0))
                //连接以后该干什么
                .process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    //如何写泛型
                    private ListState<Tuple2<String,Integer>> list1;
                    private ListState<Tuple2<String,Integer>> list2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //如何初始化:Context;state;describe
                        list1=getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple2<String,Integer>>("list-1", Types.TUPLE(Types.STRING,Types.INT))
                                //这个沙雕tuple类怎么这么难写,还有沙雕泛型
                        );
                        list2=getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple2<String,Integer>>("list-2", Types.TUPLE(Types.STRING,Types.INT)
                                )
                        );
                    }//open方法写好以后

                    @Override
                    public void processElement1(Tuple2<String, Integer> left, Context ctx, Collector<String> out) throws Exception {
                    //此方法处理list1,怎么整:将传入value加到list1中
                        list1.add(left);
                        for (Tuple2<String,Integer> right:list2.get()
                             ) {
                            //调用上下文还是调用收集方法:收集方法collect,因为你要打印:=>进行输出
                            out.collect(left+" => "+right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, Integer> left, Context ctx, Collector<String> out) throws Exception {
                        //处理list2,和①的 逻辑一样
                        list2.add(left);
                        for (Tuple2<String,Integer> right:list1.get()
                        ){
                            out.collect(left+"=>"+right);
                        }

                    }

                })
                .print();
        //成了
        env.execute();


    }
}
