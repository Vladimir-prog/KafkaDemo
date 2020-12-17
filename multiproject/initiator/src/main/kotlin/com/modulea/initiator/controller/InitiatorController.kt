package com.modulea.initiator.controller

import com.modulea.initiator.service.InitiatorService
import com.modulea.initiator.model.Request
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@RestController
class InitiatorController(private val service: InitiatorService) {

    @GetMapping("/api/{requestedPath}")
    fun writeRequest(@PathVariable requestedPath: String) {
        service.initiateCommunication(Request(path = requestedPath))
    }

    @GetMapping("/api/requests/last/{number}")
    fun listLastNRequests(@PathVariable number: Long): List<Request>? {
        return service.listLastN(number)
    }

}