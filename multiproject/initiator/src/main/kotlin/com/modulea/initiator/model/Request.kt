package com.modulea.initiator.model

import java.time.LocalDateTime
import javax.persistence.Entity
import javax.persistence.GeneratedValue
import javax.persistence.Id

@Entity
class Request(
        var sent: LocalDateTime = LocalDateTime.now(),
        var path: String,
        var status: String = "waiting",
        @Id @GeneratedValue var id: Long? = null
)