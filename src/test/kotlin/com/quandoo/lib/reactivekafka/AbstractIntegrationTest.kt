/**
 *    Copyright (C) 2019 Quandoo GbmH (account.oss@quandoo.com)
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
package com.quandoo.lib.reactivekafka

import java.io.File
import java.util.UUID
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit.jupiter.SpringExtension
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderRecord

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ExtendWith(SpringExtension::class)
abstract class AbstractIntegrationTest {

    @Autowired
    lateinit var producer: KafkaSender<String, Any>

    companion object {
        private val log = LoggerFactory.getLogger(AbstractIntegrationTest::class.java)

        init {
            val testContainer = DockerComposeContainer<Nothing>(File("src/test/resources/docker-compose.yaml")).apply {
                withLocalCompose(true)
                withExposedService("kafka", 9092)
                // withLogConsumer("kafka") { log.info("KAFKA: " + it.utf8String)}
                waitingFor("kafka", LogMessageWaitStrategy().withRegEx(".*Created topic testentity2.*\\n"))
            }

            testContainer.start()
        }
    }

    protected fun sendMessageToKafka(topic: String, data: Any?, headers: Iterable<Header> = emptyList()) {
        producer.send(
                Flux.just(
                        SenderRecord.create(
                                ProducerRecord(
                                        topic,
                                        null,
                                        null,
                                        UUID.randomUUID().toString(),
                                        data,
                                        headers
                                ),
                                null
                        )
                )
        ).blockLast()
    }
}
