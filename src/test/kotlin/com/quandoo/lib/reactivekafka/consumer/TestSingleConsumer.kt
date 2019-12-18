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
import io.reactivex.Completable
import io.reactivex.schedulers.Schedulers
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils

class TestSingleConsumer() {

    companion object {
        private val log = LoggerFactory.getLogger(TestSingleConsumer::class.java)
    }

    val messageCounter = AtomicInteger(0)
    val messageMap = ConcurrentHashMap<TestEntity1, Int>()
    val messages = ConcurrentLinkedDeque<TestEntity1>()
    val fail = ConcurrentHashMap<Int, Int>()

    private val scheduler = Schedulers.from(Executors.newSingleThreadExecutor())

    @KafkaListener(topics = ["testentity1"], valueType = TestEntity1::class)
    fun process(message: ConsumerRecord<String, TestEntity1>): Completable {
        log.info("Message received: {}", message.value())
        Thread.sleep(50)

        val value = message.value() ?: TestEntity1(RandomStringUtils.randomAlphanumeric(20), RandomStringUtils.randomNumeric(5).toInt())
        messageMap.putIfAbsent(value, messageCounter.getAndIncrement())
        if ((fail[messageMap[value]] ?: 0) > 0) {
            fail[messageMap[value]!!] = fail[messageMap[value]]!! - 1
            throw RuntimeException("Failing on purpose")
        }

        messages.add(value)
        return Completable.complete().observeOn(scheduler)
    }
}
