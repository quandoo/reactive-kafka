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

import com.quandoo.lib.reactivekafka.AbstractIntegrationTest
import com.quandoo.lib.reactivekafka.KafkaLagChecker
import java.time.Duration
import org.apache.kafka.common.header.internals.RecordHeader
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

internal class KafkaConsumerIntegrationTest : AbstractIntegrationTest() {

    private val topic1 = "testentity1"
    private val topic2 = "testentity2"

    @Autowired
    private lateinit var testSingleConsumer: TestSingleConsumer

    @Autowired
    private lateinit var testBatchConsumer: TestBatchConsumer

    @Autowired
    private lateinit var kafkaLagChecker: KafkaLagChecker

    @BeforeEach
    internal fun setUp() {
        testSingleConsumer.messages.clear()
        testBatchConsumer.messages.clear()
    }

    @AfterEach
    internal fun tearDown() {
        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(kafkaLagChecker.areConsumersLagging()).isEqualTo(false)
        }
    }

    @Test
    fun `SingleHandler - Test messages successfully processed`() {
        for (i in 1..101) {
            sendMessageToKafka(topic1, TestEntity1("message$i", i))
        }

        await().atMost(Duration.ofSeconds(20L)).untilAsserted {
            assertThat(testSingleConsumer.messages).hasSize(101)
        }
    }

    @Test
    fun `SingleHandler - Test empty body message successfully processed`() {
        sendMessageToKafka(topic1, null)

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testSingleConsumer.messages).hasSize(1)
        }
    }

    @Test
    fun `SingleHandler - Test message successfully filtered out by filter`() {
        val message1 = TestEntity1("message1", 1)
        val message2 = TestEntity1("message2", 1)
        val message3 = TestEntity1("xyz", 1)

        sendMessageToKafka(topic1, message1)
        sendMessageToKafka(topic1, message2)
        sendMessageToKafka(topic1, message3)

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testSingleConsumer.messages)
                    .hasSize(2)
                    .contains(message1, message2)
        }
    }

    @Test
    fun `SingleHandler - Test all message successfully filtered out by filter`() {
        val message1 = TestEntity1("xyz", 1)
        val message2 = TestEntity1("xyz", 1)
        val message3 = TestEntity1("xyz", 1)

        sendMessageToKafka(topic1, message1)
        sendMessageToKafka(topic1, message2)
        sendMessageToKafka(topic1, message3)

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testSingleConsumer.messages).hasSize(0)
        }
    }

    @Test
    @ExperimentalStdlibApi
    fun `SingleHandler - Test message successfully filtered out by pre-filter`() {
        val message1 = TestEntity1("message1", 1)
        val message2 = TestEntity1("message2", 1)
        val message3 = TestEntity1("message3", 1)

        sendMessageToKafka(topic1, message1)
        sendMessageToKafka(topic1, message2)
        sendMessageToKafka(topic1, message3, listOf(RecordHeader("version", "99".encodeToByteArray())))

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testSingleConsumer.messages)
                    .hasSize(2)
                    .contains(message1, message2)
        }
    }

    @Test
    @ExperimentalStdlibApi
    fun `SingleHandler - Test all message successfully filtered out by pre-filter`() {
        val message1 = TestEntity1("message1", 1)
        val message2 = TestEntity1("message2", 1)
        val message3 = TestEntity1("message3", 1)

        sendMessageToKafka(topic1, message1, listOf(RecordHeader("version", "99".encodeToByteArray())))
        sendMessageToKafka(topic1, message2, listOf(RecordHeader("version", "99".encodeToByteArray())))
        sendMessageToKafka(topic1, message3, listOf(RecordHeader("version", "99".encodeToByteArray())))

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testSingleConsumer.messages).hasSize(0)
        }
    }

    @Test
    fun `SingleHandler- Test message retried until successfully processed`() {
        val message1 = TestEntity1("message1", 1)
        val message2 = TestEntity1("message2", 1)
        val message3 = TestEntity1("message3", 1)

        testSingleConsumer.fail[1] = 5

        sendMessageToKafka(topic1, message1)
        sendMessageToKafka(topic1, message2)
        sendMessageToKafka(topic1, message3)

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testSingleConsumer.messages)
                    .hasSize(3)
                    .contains(message1, message2, message3)
        }
    }

    @Test
    fun `BatchHandler - Test messages successfully processed`() {
        for (i in 1..101) {
            sendMessageToKafka(topic2, TestEntity2("message$i", i))
        }

        await().atMost(Duration.ofSeconds(20L)).untilAsserted {
            assertThat(testBatchConsumer.messages).hasSize(101)
        }
    }

    @Test
    fun `BatchHandler - Test empty message successfully processed`() {
        sendMessageToKafka(topic2, null)

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testBatchConsumer.messages).hasSize(1)
        }
    }

    @Test
    fun `BatchHandler - Test message successfully filtered out by filter`() {
        val message1 = TestEntity2("message1", 1)
        val message2 = TestEntity2("message2", 1)
        val message3 = TestEntity2("xyz", 1)

        sendMessageToKafka(topic2, message1)
        sendMessageToKafka(topic2, message2)
        sendMessageToKafka(topic2, message3)

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testBatchConsumer.messages)
                    .hasSize(2)
                    .contains(message1, message2)
        }
    }

    @Test
    fun `BatchHandler - Test all message successfully filtered out by filter`() {
        val message1 = TestEntity2("xyz", 1)
        val message2 = TestEntity2("xyz", 1)
        val message3 = TestEntity2("xyz", 1)

        sendMessageToKafka(topic2, message1)
        sendMessageToKafka(topic2, message2)
        sendMessageToKafka(topic2, message3)

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testBatchConsumer.messages).hasSize(0)
        }
    }

    @Test
    @ExperimentalStdlibApi
    fun `BatchHandler - Test message successfully filtered out by pre-filter`() {
        val message1 = TestEntity2("message1", 1)
        val message2 = TestEntity2("message2", 1)
        val message3 = TestEntity2("message3", 1)

        sendMessageToKafka(topic2, message1)
        sendMessageToKafka(topic2, message2)
        sendMessageToKafka(topic2, message3, listOf(RecordHeader("version", "99".encodeToByteArray())))

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testBatchConsumer.messages)
                    .hasSize(2)
                    .contains(message1, message2)
        }
    }

    @Test
    @ExperimentalStdlibApi
    fun `BatchHandler - Test all message successfully filtered out by pre-filter`() {
        val message1 = TestEntity2("message1", 1)
        val message2 = TestEntity2("message2", 1)
        val message3 = TestEntity2("message3", 1)

        sendMessageToKafka(topic2, message1, listOf(RecordHeader("version", "99".encodeToByteArray())))
        sendMessageToKafka(topic2, message2, listOf(RecordHeader("version", "99".encodeToByteArray())))
        sendMessageToKafka(topic2, message3, listOf(RecordHeader("version", "99".encodeToByteArray())))

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testBatchConsumer.messages).hasSize(0)
        }
    }

    @Test
    fun `BatchHandler- Test message retried until successfully processed`() {
        val message1 = TestEntity2("message1", 1)
        val message2 = TestEntity2("message2", 1)
        val message3 = TestEntity2("message3", 1)

        testSingleConsumer.fail[1] = 5

        sendMessageToKafka(topic2, message1)
        sendMessageToKafka(topic2, message2)
        sendMessageToKafka(topic2, message3)

        await().atMost(Duration.ofSeconds(10L)).untilAsserted {
            assertThat(testBatchConsumer.messages)
                    .hasSize(3)
                    .contains(message1, message2, message3)
        }
    }
}
