package com.aliendt.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example04EleParall {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Integer>> key1 = env
                .fromElements(1, 2, 3, 4).setParallelism(1)
                .map(r -> Tuple2.of("key", r)).setParallelism(4)
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        key1.print("============我是key1========================");
        SingleOutputStreamOperator<Tuple2<String, Integer>> key2 = key1
                //类型擦除,所以要返回类型让别人知道知道我是什么样的人
                .keyBy(r -> r.f0)
                .sum(1).setParallelism(2);
        key2
                .print("==========我是key2==================").setParallelism(4);

        env.execute();
    }
}
