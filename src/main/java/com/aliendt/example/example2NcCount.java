package com.aliendt.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class example2NcCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.socketTextStream("hadoop202", 9999);
        SingleOutputStreamOperator<WordWithCount> mapstream = stream.flatMap(
                new FlatMapFunction<String, WordWithCount>() {
                    //为什么这样new匿名内部类:泛型的的定义类标准为输入和输出
                    /*单写类为什么会报错,因为学flatmap的时候说的就是给一个容器,返回一个容器
                    你说为什么写Collection
                    大傻蛋,匿名内部类的泛型和方法中的参数是不一样的
                     */
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        String[] split = value.split(" ");
                        for (String s :
                                split) {
                            /*在写RDD的时候在flatmap中会套用map
                             */
                            out.collect(new WordWithCount(s, 1L));
                        }
                    }
                });
        System.out.println("==========2===========");
        //mapstream.print();
        System.out.println("========1========");
        KeyedStream<WordWithCount, String> kedstream = mapstream.keyBy(
                //为什么要用keyby,因为在spark中这步会用reduceby
                new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount value) throws Exception {
                        /*这个传入参数的变量名,和WordWithCont中的属性重名,为什么会重名
                        因为这个方法被重写之前的传入变量就是value
                         */
                        return value.word;
                    }
                }

        );
        SingleOutputStreamOperator<WordWithCount> result = kedstream.reduce(
                new ReduceFunction<WordWithCount>() {
                    /*上文说到,之前用reducebyey就能一步搞定,but这里用两步,一是reduce一是keyb
                     */
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word, value1.count + value2.count);
                        //无参构造传入参数
                        //返回值类型看什么,看泛型呀,我的天
                    }
                }
        );
        result.print();
        //什么样的类型才可以print出来, 这里是SingleOutputStreamOperator,为啥我打印不出来,废话sout能打印个屁
        //通过测验,确实是在打印累加器的状态
        env.execute();
    }


    public static class WordWithCount{
        public String word;
        public Long count;

        public WordWithCount() {
        }

        public WordWithCount(String value, Long count) {
            this.word = value;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "value='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
