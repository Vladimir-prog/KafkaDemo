package com.modulea.initiator.service

import com.modulea.initiator.config.ConsumerConfig
import com.modulea.initiator.config.ConsumerConfig.Companion.TOPIC
import com.modulea.initiator.jsonMapper
import com.modulea.initiator.model.Request
import com.modulea.initiator.repo.RequestRepository
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

@Service
class ConsumerService(private val requestRepo: RequestRepository) {

    private val logger = LoggerFactory.getLogger(ConsumerService::class.java)
    private val set = ConcurrentHashMap.newKeySet<Long>()
    private val lock = ReentrantLock()

    @KafkaListener(topics = [ConsumerConfig.TOPIC], groupId = "a_module_consumer")
    fun receive(msg: String) {
        logger.info("RECEIVED REQUEST $msg FROM TOPIC $TOPIC")

        val reply = jsonMapper.readValue(msg, LocalDateTime::class.java)
        val found = requestRepo.findBySent(reply)

        if (found?.id != null) {

            lock.lock()
            try {
                if (set.contains(found.id) || checkUpdateRequest(found)) {
                    return
                }

                set.add(found.id)
            } catch (ex: Exception) {
                logger.error("error: ", ex)
                return
            } finally {
                lock.unlock()
            }

            found.status = "successful"
            try {
                update(found)
            } catch (ex: Exception) {
                logger.error("error: ", ex)
                return
            } finally {
                set.remove(found.id)
            }

            logger.info("UPDATED REQUEST SENT AT $reply STATUS")
        } else {
            logger.info("REQUEST SENT AT $reply NOT FOUND")
        }
    }

    @Transactional
    private fun update(request: Request) {
        requestRepo.save(request)
    }

    private fun checkUpdateRequest(request: Request): Boolean {
        return requestRepo.findById(request.id!!)
            .filter { entity -> entity.status != "waiting" }
            .isPresent
    }

}
