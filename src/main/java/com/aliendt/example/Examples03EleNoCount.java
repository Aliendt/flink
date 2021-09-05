package com.aliendt.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Examples03EleNoCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WordWithCount> resoult = env
                .fromElements("hello friend", "hello flink", "hello spark")
                .flatMap(
                        new FlatMapFunction<String, WordWithCount>() {
                            @Override
                            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                                String[] split = value.split(" ");
                                for (String s :
                                        split) {
                                    out.collect(new WordWithCount(s, 1l));

                                }
                            }
                        }
                )
                .keyBy(
                        new KeySelector<WordWithCount, String>() {
                            @Override
                            public String getKey(WordWithCount value) throws Exception {
                                return value.word;
                            }
                        }
                )
                //.keyBy(r->r.count)一样
                .reduce(
                        new ReduceFunction<WordWithCount>() {
                            @Override
                            public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                                return new WordWithCount(value1.word, value1.count + value2.count);
                            }
                        }
                );
        resoult.print();
        env.execute();
    }
    public static class WordWithCount{
        public String word;
        public Long count;

       

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }
        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
