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

import kotlin.reflect.KClass

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class KafkaListener(
    val topics: Array<String>,
    val valueType: KClass<*>,

    val enabled: String = "true",
    val groupId: String = "",
    val batchSize: String = "",
    val parallelism: String = "",
    val maxPoolIntervalMillis: String = "",
    val commitInterval: String = "",
    val commitBatchSize: String = "",
    val retryBackoffMillis: String = "",
    val heartBeatIntervalMillis: String = "",
    val sessionTimeoutMillis: String = "",
    val partitionAssignmentStrategy: String = "",
    val autoOffsetReset: String = ""
)
