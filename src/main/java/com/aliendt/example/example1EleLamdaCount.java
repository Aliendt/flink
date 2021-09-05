package com.aliendt.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class example1EleLamdaCount {
    public static void main(String[] args) throws  Exception{
        //简单粗暴
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度忘球设置了
        env.setParallelism(1);
        env
                .fromElements("hello oh","word","hello flink","hello bigdata")
                .flatMap(
                        (String value, Collector<WordWithCount> out)->{
                            String[] arr = value.split(" ");
                            for (String s:
                                 arr) {
                                out.collect(new WordWithCount(s,1L));

                            }
                        }
                )
                .returns(Types.POJO(WordWithCount.class))
                .keyBy(r->r.word)
                .reduce((WordWithCount v1,WordWithCount v2)->new WordWithCount(v1.word,v1.count+ v2.count))
                .print();
        env.execute();



    }
    public static class WordWithCount{
        public String word;
        public Long count;

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        public WordWithCount() {
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
