package com.moduleb.receiver.model

import java.time.LocalDateTime
import javax.persistence.Entity
import javax.persistence.Id

@Entity
class Message(
        val sent: LocalDateTime,
        @Id
        val id: Long
)