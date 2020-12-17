package com.moduleb.receiver.model

import java.time.LocalDateTime


class Request(
        var sent: LocalDateTime,
        var path: String,
        var status: String,
        var id: Long
)