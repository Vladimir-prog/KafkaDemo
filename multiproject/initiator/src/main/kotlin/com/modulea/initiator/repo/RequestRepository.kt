package com.modulea.initiator.repo

import com.modulea.initiator.model.Request
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import java.time.LocalDateTime

interface RequestRepository : JpaRepository<Request, Long> {

    fun findBySent(param: LocalDateTime): Request?

    //TODO Maybe use Pageable instead?
    @Query("SELECT * FROM Request ORDER BY ID DESC LIMIT :num", nativeQuery = true)
    fun list(num: Long): List<Request>
}