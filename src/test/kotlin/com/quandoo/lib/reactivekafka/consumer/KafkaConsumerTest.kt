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

import com.quandoo.lib.reactivekafka.KafkaProperties
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerMeta
import io.reactivex.Completable
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class KafkaConsumerTest {

    @Test
    fun `Test that KafkaProperties without consumer throws exception`() {
        val kafkaProperties = KafkaProperties()
        kafkaProperties.bootstrapServers = "localhost:9200"

        val exception = assertThrows<IllegalStateException> { KafkaConsumer(kafkaProperties, listOf()) }
        assertThat(exception.message).isEqualTo("At least one consumer has to be defined")
    }

    @Test
    fun `Test that non unique Entity listener throws exception`() {
        val kafkaProperties = KafkaProperties()
        val consumer = KafkaProperties.KafkaConsumerProperties()
        consumer.autoOffsetReset = "earliest"
        consumer.groupId = "test"

        kafkaProperties.bootstrapServers = "localhost:9200"
        kafkaProperties.consumer = consumer

        val kafkaListenerMetas = listOf(
                KafkaListenerMeta(
                        handler = object : SingleHandler {
                            override fun apply(value: ConsumerRecord<*, *>): Any {
                                return Completable.complete()
                            }
                        },
                        topics = listOf("topic1"),
                        keyClass = String::class.java,
                        valueClass = String::class.java,
                        keyDeserializer = StringDeserializer(),
                        valueDeserializer = StringDeserializer(),
                        groupId = "test"
                ),
                KafkaListenerMeta(
                        handler = object : SingleHandler {
                            override fun apply(value: ConsumerRecord<*, *>): Any {
                                return Completable.complete()
                            }
                        },
                        topics = listOf("topic1"),
                        keyClass = String::class.java,
                        valueClass = String::class.java,
                        keyDeserializer = StringDeserializer(),
                        valueDeserializer = StringDeserializer(),
                        groupId = "test"
                )
        )

        val exception = assertThrows<IllegalStateException> { KafkaConsumer(kafkaProperties, kafkaListenerMetas) }
        assertThat(exception.message).isEqualTo("Only one listener per groupID and Entity can be defined")
    }
}
