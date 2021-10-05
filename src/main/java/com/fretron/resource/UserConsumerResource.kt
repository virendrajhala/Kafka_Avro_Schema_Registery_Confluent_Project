package com.fretron.resource

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

class UserConsumerResource {

}

fun main(){

    val scanner = Scanner(System.`in`)
    print("Enter topic name : ")
    val topicName = scanner.nextLine()


    val consumer = KafkaConsumer<String,String>(getConsumerProperties())

    //Kafka Consumer subscribes to  a list of topics here.
    consumer.subscribe(Arrays.asList(topicName))

    //print the topic name
    println("Subscribed to topic $topicName")

    while (true){
        val consumerRecords = consumer.poll(100)

        for(record in consumerRecords){

           // Thread.sleep(3000)
            // print the offset,key and value for the consumer records.
            println("Offset : ${record.offset()},     Key : ${record.key()},    Value : ${record.value()}")


        }
    }
}

fun getConsumerProperties(): Properties {
    //Kafka Consumer Config setting
    val props = Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");


    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
//    props.put(
//        "key.deserializer",
//        "io.confluent.kafka.serializers.KafkaAvroDeserializer"
//    )
//    props.put(
//        "value.deserializer",
//        "io.confluent.kafka.serializers.KafkaAvroDeserializer"
//    )

   // props.put("AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG","localhost:9092")
    props.put("schema.registry.url", "http://0.0.0.0:8081");

    return props
}