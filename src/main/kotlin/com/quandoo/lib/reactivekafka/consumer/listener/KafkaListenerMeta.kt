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
package com.quandoo.lib.reactivekafka.consumer.listener

import com.google.common.base.Predicate
import com.google.common.base.Predicates
import com.quandoo.lib.reactivekafka.consumer.Handler
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.Bytes

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
data class KafkaListenerMeta<K, V>(
    val handler: Handler<*, *>,
    val topics: List<String>,
    val keyClass: Class<K>,
    val valueClass: Class<V>,
    val keyDeserializer: Deserializer<K>,
    val valueDeserializer: Deserializer<V>,
    val preFilter: Predicate<in ConsumerRecord<Bytes, Bytes>> = Predicates.alwaysTrue(),
    val filter: Predicate<in ConsumerRecord<in K, in V>> = Predicates.alwaysTrue(),

    // Optional properties
    val groupId: String? = null,
    val batchSize: Int? = null,
    val parallelism: Int? = null,
    val maxPoolIntervalMillis: Int? = null,
    val batchWaitMillis: Long? = null,
    val retryBackoffMillis: Long? = null,
    val partitionAssignmentStrategy: String? = null,
    val autoOffsetReset: String? = null
)
