package com.fretron.resource

import com.fretron.model.User
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import java.util.concurrent.TimeUnit

class UserProducerResource {
}

fun main() {

    val scanner = Scanner(System.`in`)
    print("Enter topic name : ")
    val topicName = scanner.nextLine()

    val producer = KafkaProducer<String, String>(getProperties())
    val userList = getUserList()
    while(userList.size>0) {
        for(user in userList) {
            println(user)
            val producerRecord = ProducerRecord(topicName, user.getId().toString(), user.toString())
            val future = producer.send(producerRecord)
            val res = future.get(2000L ,TimeUnit.MILLISECONDS)
            println("Prodcued with offset"+res.offset())

             Thread.sleep(2000)
        }
    }

    ("Message sent successfully")

    producer.close()


}

fun getProperties(): Properties {
    // create instance for properties to access producer configs
    val props = Properties()
    //Assign localhost id
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java

    //Set acknowledgements for producer requests.
    props.put("acks", "all")

    //If the request fails, the producer can automatically retry,
    props.put("retries", 0)

    //Specify buffer size in config
    props.put("batch.size", 16384)

    //time to wait before sending msgs to Kafka
    props.put("linger.ms", 0)

    //The buffer.memory - the total amount of memory available to the producer for buffering.
    props.put("buffer.memory", 33554432)

//    props.put(
//        "key.serializer",
//        "io.confluent.kafka.serializers.KafkaAvroSerializer"
//    )
//
//    props.put(
//        "value.serializer",
//        "io.confluent.kafka.serializers.KafkaAvroSerializer"
//    )

   // props.put("AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG","localhost:9092")

    props.put("schema.registry.url", "http://0.0.0.0:8081");

    return props
}

fun getUserList():MutableList<User>{

    val userList = mutableListOf<User>()
    var j:Long = 1
    var name = "a"
    var address = "b"
    var email = "xyz@gmail.com"
    var a = 58
    var age = a % 35

    for(i in 1..100){

        val user = getUser(j,name,address,email,age)

        userList.add(user)

        j++
        name=name.plus("a")
        address=address.plus("b")
        email=email.plus("c")
        if(a<69)
        a++

        else
            a=58

        age = a % 35
    }

    return userList
}

fun getUser(id:Long,name:String,address: String,email:String,age:Int):User{
    return User(id,name,address,email,age)

}
