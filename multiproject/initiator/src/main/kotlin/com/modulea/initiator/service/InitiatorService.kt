package com.modulea.initiator.service

import com.modulea.initiator.config.ProducerConfig.Companion.topic
import com.modulea.initiator.model.Request
import com.modulea.initiator.repo.RequestRepository
import com.modulea.initiator.jsonMapper
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class InitiatorService(private val requestRepository: RequestRepository,
                       private val prod: Producer<String, String>
) {
    private val logger = LoggerFactory.getLogger(InitiatorService::class.java)

    //TODO decouple?
    fun initiateCommunication(request: Request) {
        requestRepository.save(request)
        publishAndListen(request)
    }

    fun publishAndListen(req: Request) {
        prod.send(ProducerRecord(topic, jsonMapper.writeValueAsString(req)))
        logger.info("SENT ${req.path} REQUEST MADE AT ${req.sent} TO $topic")
    }

    fun listLastN(num: Long): List<Request>? {
        return requestRepository.list(num)
    }

}
