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

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

class KafkaLagChecker(private val kafkaProperties: KafkaProperties) {

    private val adminClient: AdminClient = AdminClient.create(mapOf(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers
    ))

    private val consumer = createNewConsumer(kafkaProperties)

    fun areConsumersLagging(): Boolean {
        val consumerGroups = adminClient.listConsumerGroups().all().get()
        val groupIdOffsets = consumerGroups
                .map { adminClient.listConsumerGroupOffsets(it.groupId()).partitionsToOffsetAndMetadata().get() }
        val topicOffsets = getEndOffset(
                adminClient.describeConsumerGroups(consumerGroups.map { it.groupId() }.toList()).all().get()
                        .flatMap { it.value.members().flatMap { member -> member.assignment().topicPartitions() } }
                        .toSet()
        )

        return if(topicOffsets.isEmpty()) {
            true
        } else {
            groupIdOffsets.flatMap { groupIdOffset ->
                groupIdOffset.map { groupIdOffsetEntry ->
                    val topicOffset = topicOffsets[groupIdOffsetEntry.key]!!
                    groupIdOffsetEntry.value.offset() < topicOffset
                }
            }.fold(false) { a, b -> a || b }
        }
    }

    private fun getEndOffset(topicPartitions: Set<TopicPartition>): Map<TopicPartition, Long> {
        return consumer.endOffsets(topicPartitions)
    }

    private fun createNewConsumer(kafkaProperties: KafkaProperties): KafkaConsumer<String, String> {
        return KafkaConsumer(
                mapOf(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG to "test-group",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to "30000",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringDeserializer",
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringDeserializer"
                )
        )
    }
}
