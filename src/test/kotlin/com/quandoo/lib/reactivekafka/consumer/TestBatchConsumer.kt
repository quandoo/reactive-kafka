/**
 *    Copyright (C) 2019 Quandoo GmbH (account.oss@quandoo.com)
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */
package com.quandoo.lib.reactivekafka.consumer

import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListener
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

class TestBatchConsumer() {

    companion object {
        private val log = LoggerFactory.getLogger(TestBatchConsumer::class.java)
    }

    val messageCounter = AtomicInteger(0)
    val messageMap = ConcurrentHashMap<TestEntity2, Int>()
    val messages = ConcurrentLinkedDeque<TestEntity2>()
    val fail = ConcurrentHashMap<Int, Int>()

    private val scheduler = Schedulers.newSingle("batch-listener")

    @Synchronized
    @KafkaListener(topics = ["testentity2"], valueType = TestEntity2::class)
    fun process(kafkaMessages: List<ConsumerRecord<String, TestEntity2>>): Mono<Void> {
        log.info("Messages received: {}", kafkaMessages.map { it.value() })
        Thread.sleep(500)

        kafkaMessages.forEach {
            val value = it.value() ?: TestEntity2(RandomStringUtils.randomAlphanumeric(20), RandomStringUtils.randomNumeric(5).toInt())
            messageMap.putIfAbsent(value, messageCounter.getAndIncrement())
            if ((fail[messageMap[value]] ?: 0) > 0) {
                fail[messageMap[value]!!] = fail[messageMap[it.value()]]!! - 1
                throw RuntimeException("Failing on purpose")
            }

            messages.add(value)
        }

        return Mono.empty<Void>().publishOn(scheduler)
    }
}
