package com.moduleb.receiver.repo

import com.moduleb.receiver.model.Message
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import java.time.LocalDateTime

interface MessageRepository : JpaRepository<Message, Long> {

    fun findTopByOrderByIdDesc(): Message?

    @Query("SELECT m.sent FROM Message m WHERE m.id = ?1")
    fun findTimeById(id: Long): LocalDateTime
}