package com.moduleb.receiver.config

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import java.util.*

@EnableKafka
@Configuration
class ConsumerConfig {

    @Bean
    fun createConsumer(): Consumer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        //props["group.id"] = "b_module_consumer"
        props["key.deserializer"] = StringDeserializer::class.java
        props["value.deserializer"] = StringDeserializer::class.java
        return KafkaConsumer<String, String>(props)
    }

    companion object {
        const val TOPIC = "first"
    }
}

