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
package com.quandoo.lib.reactivekafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.common.base.Predicate
import com.quandoo.lib.reactivekafka.consumer.TestBatchConsumer
import com.quandoo.lib.reactivekafka.consumer.TestDisabledConsumer
import com.quandoo.lib.reactivekafka.consumer.TestEntity1
import com.quandoo.lib.reactivekafka.consumer.TestEntity2
import com.quandoo.lib.reactivekafka.consumer.TestSingleConsumer
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerFilter
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerPreFilter
import com.quandoo.lib.reactivekafka.spring.KafkaAutoconfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.utils.Bytes
import org.springframework.boot.SpringBootConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import

@Import(KafkaAutoconfiguration::class)
@SpringBootConfiguration
internal class TestConfiguration {

    @Bean
    fun objectMapper(): ObjectMapper {
        return ObjectMapper().registerModule(KotlinModule())
    }

    @Bean
    fun testSingleConsumer(): TestSingleConsumer {
        return TestSingleConsumer()
    }

    @Bean
    fun testDisabledConsumer(): TestDisabledConsumer {
        return TestDisabledConsumer()
    }

    @Bean
    fun testBatchConsumer(): TestBatchConsumer {
        return TestBatchConsumer()
    }

    @Bean
    fun kafkaConsumerFilter1(): Predicate<ConsumerRecord<String, TestEntity1>> {
        return TestFilter1()
    }

    @Bean
    fun kafkaConsumerFilter2(): Predicate<ConsumerRecord<String, TestEntity2>> {
        return TestFilter2()
    }

    @Bean
    fun kafkaConsumerPreFilter(): Predicate<ConsumerRecord<Bytes, Bytes>> {
        return TestPreFilter()
    }

    @Bean
    fun kafkaLagChecker(kafkaProperties: KafkaProperties): KafkaLagChecker {
        return KafkaLagChecker(kafkaProperties)
    }
}

@KafkaListenerFilter(valueClass = TestEntity1::class)
class TestFilter1 : Predicate<ConsumerRecord<String, TestEntity1>> {
    override fun apply(receiverRecord: ConsumerRecord<String, TestEntity1>?): Boolean {
        return receiverRecord!!.value()?.let { it.value != "xyz" } ?: true
    }
}

@KafkaListenerFilter(valueClass = TestEntity2::class)
class TestFilter2 : Predicate<ConsumerRecord<String, TestEntity2>> {
    override fun apply(receiverRecord: ConsumerRecord<String, TestEntity2>?): Boolean {
        return receiverRecord!!.value()?.let { it.value != "xyz" } ?: true
    }
}

@KafkaListenerPreFilter
class TestPreFilter : Predicate<ConsumerRecord<Bytes, Bytes>> {
    override fun apply(receiverRecord: ConsumerRecord<Bytes, Bytes>?): Boolean {
        return receiverRecord!!.headers().lastHeader("version")?.let { String(it.value()) < "100" } ?: true
    }
}
