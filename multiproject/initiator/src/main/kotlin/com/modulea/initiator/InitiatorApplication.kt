package com.modulea.initiator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class InitiatorApplication

fun main(args: Array<String>) {
    runApplication<InitiatorApplication>(*args)
}
