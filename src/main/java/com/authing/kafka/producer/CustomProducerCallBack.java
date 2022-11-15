package com.authing.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author rebel
 * @date 2022/11/14 16:18
 */
public class CustomProducerCallBack {

    public static void main(String[] args) {

        //0.配置
        Properties properties=new Properties();

        //连接集群 即各个broker  bootstrap.servers
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");

        //指定对应的key和value的序列化类型 key.serializer
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //2.发送数据
        for (int i = 1; i <=5 ; i++) {
            //a.只传topic和value
            kafkaProducer.send(new ProducerRecord<String, String>("first", "第" + String.valueOf(i) + "条数据"), new Callback() {
                //回调函数
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        System.out.println("主题为："+recordMetadata.topic()+"发送成功，分区信息为："+recordMetadata.partition());

                    }
                }
            });

        }

        //3.关闭资源
        kafkaProducer.close();

    }
}
