package com.aliendt.example;



import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

// 从文件读取并写入kafka
public class TeacherKafkaProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop202:9092");

        env
                .readTextFile("D:\\java\\Cade\\flink\\src\\main\\resources\\UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "user-behavior-topic1",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}

