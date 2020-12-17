package com.modulea.initiator.service

import com.modulea.initiator.config.ConsumerConfig
import com.modulea.initiator.config.ConsumerConfig.Companion.TOPIC
import com.modulea.initiator.repo.RequestRepository
import com.modulea.initiator.jsonMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import java.time.LocalDateTime

@Service
class ConsumerService(private val requestRepo: RequestRepository) {

    private val logger = LoggerFactory.getLogger(ConsumerService::class.java)

    @KafkaListener(topics = [ConsumerConfig.TOPIC], groupId = "a_module_consumer")
    fun receive(msg: String) {
        logger.info("RECEIVED REQUEST $msg FROM TOPIC $TOPIC")

        val reply = jsonMapper.readValue(msg, LocalDateTime::class.java)
        var found = requestRepo.findBySent(reply)
        if (found != null) {
            found.status = "successful"
            requestRepo.save(found)
            logger.info("UPDATED REQUEST SENT AT $reply STATUS")
        } else {
            logger.info("REQUEST SENT AT $reply NOT FOUND")
        }
    }
}
