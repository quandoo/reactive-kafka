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

import com.google.common.base.MoreObjects
import com.google.common.base.Predicate
import com.quandoo.lib.reactivekafka.KafkaProperties
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerMeta
import com.quandoo.lib.reactivekafka.util.KafkaConfigHelper
import io.reactivex.Completable
import io.reactivex.Flowable
import io.reactivex.Single
import io.reactivex.subscribers.DisposableSubscriber
import java.time.Duration
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.Bytes
import org.slf4j.LoggerFactory
import reactor.adapter.rxjava.RxJava2Adapter
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
class KafkaConsumer(private val kafkaProperties: KafkaProperties, listeners: List<KafkaListenerMeta<*, *>>) {

    companion object {
        private val log = LoggerFactory.getLogger(KafkaConsumer::class.java)
    }

    private val kafkaListenerProperties: List<KafkaListenerProperties<*, *>> = (listeners
            .takeIf { it.isNotEmpty() } ?: error("At least one consumer has to be defined"))
            .groupBy { it.groupId }
            .flatMap { entry ->
                check(entry.value.map { it.valueClass }.size == entry.value.map { it.valueClass }.toSet().size) { "Only one listener per groupID and Entity can be defined" }
                entry.value.map { mergeConfiguration(it, kafkaProperties) }
            }

    fun start() {
        startConsumers()
    }

    private fun <K, V> createReceiverOptions(kafkaListenerProperties: KafkaListenerProperties<K, V>): ReceiverOptions<Bytes, Bytes> {
        val consumerProps = HashMap<String, Any>()
                .also {
                    it[ConsumerConfig.GROUP_ID_CONFIG] = kafkaListenerProperties.groupId
                    it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = kafkaListenerProperties.autoOffsetReset
                    it[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = kafkaListenerProperties.batchSize
                    it[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = kafkaListenerProperties.maxPoolIntervalMillis
                    it[ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG] = kafkaListenerProperties.partitionAssignmentStrategy
                }
                .let { KafkaConfigHelper.populateCommonConfig(kafkaProperties, it) }
                .let { KafkaConfigHelper.populateSslConfig(kafkaProperties, it) }
                .let { KafkaConfigHelper.populateSaslConfig(kafkaProperties, it) }

        return ReceiverOptions.create<Bytes, Bytes>(consumerProps)
                // Disable automatic commits
                .commitInterval(Duration.ZERO)
                .commitBatchSize(0)
                .withKeyDeserializer(BytesDeserializer())
                .withValueDeserializer(BytesDeserializer())
                .subscription(kafkaListenerProperties.topics)
                .schedulerSupplier { kafkaListenerProperties.scheduler }
    }

    @SuppressWarnings("unchecked")
    private fun startConsumers() {
        kafkaListenerProperties.forEach {
            for (i in 1..it.parallelism) {
                startConsumer(it)
            }
        }
    }

    private fun <K, V> startConsumer(kafkaListenerProperties: KafkaListenerProperties<K, V>) {
        val receiver = KafkaReceiver.create(createReceiverOptions(kafkaListenerProperties))
        Flowable.defer { RxJava2Adapter.fluxToFlowable(receiver.receive().bufferTimeout(kafkaListenerProperties.batchSize, Duration.ofMillis(kafkaListenerProperties.batchWaitMillis), kafkaListenerProperties.scheduler)) }
                .flatMap({ receiverRecords ->
                            when (kafkaListenerProperties.handler) {
                                is SingleHandler -> {
                                    processSingle(kafkaListenerProperties, receiverRecords).toFlowable<List<ReceiverRecord<*, *>>>().defaultIfEmpty(receiverRecords)
                                }
                                is BatchHandler -> {
                                    processBatch(kafkaListenerProperties, receiverRecords).toFlowable<List<ReceiverRecord<*, *>>>().defaultIfEmpty(receiverRecords)
                                }
                                else -> {
                                    throw IllegalStateException("Unknown handler type: ${kafkaListenerProperties.handler.javaClass}")
                                }
                            }
                                .observeOn(io.reactivex.schedulers.Schedulers.from { r -> kafkaListenerProperties.scheduler.schedule(r) })
                                .concatMap(
                                        { records ->
                                            if (records.isNotEmpty()) {
                                                // All offsets need to be acknowledged
                                                records.forEach { it.receiverOffset().acknowledge() }

                                                val lastReceiverRecordPerTopicPartition = records.map { it.receiverOffset().topicPartition() to it }.toMap()
                                                Flowable.fromIterable(lastReceiverRecordPerTopicPartition.values)
                                                        .flatMap { receiverRecord ->
                                                            RxJava2Adapter.monoToCompletable(receiverRecord.receiverOffset().commit()).toSingle { receiverRecord }.toFlowable()
                                                        }
                                                        .toList()
                                                        .toFlowable()
                                                        .map { records }
                                            } else {
                                                Flowable.just(records)
                                            }
                                        },
                                        1
                                )
                        },
                        1
                )
                .observeOn(io.reactivex.schedulers.Schedulers.from { r -> kafkaListenerProperties.scheduler.schedule(r) })
                .retry()
                .subscribeOn(io.reactivex.schedulers.Schedulers.from { r -> kafkaListenerProperties.scheduler.schedule(r) }, true)
                .subscribe(
                        object : DisposableSubscriber<List<ReceiverRecord<*, *>>>() {
                            override fun onStart() {
                                request(1)
                            }

                            override fun onNext(receiverRecord: List<ReceiverRecord<*, *>>) {
                                receiverRecord.forEach { logMessage("Message processed", it) }
                                request(1)
                            }

                            override fun onError(ex: Throwable) {
                                log.error("Consumer terminated", ex)
                            }

                            override fun onComplete() {
                                log.error("Consumer terminated")
                            }
                        }
                )
    }

    private fun <K, V> processSingle(
        kafkaListenerProperties: KafkaListenerProperties<K, V>,
        receiverRecords: MutableList<ReceiverRecord<Bytes, Bytes>>
    ): Completable {
        return Single.defer {
            Flowable.fromIterable(receiverRecords)
                    .filter { receiverRecord -> preFilterMessage(kafkaListenerProperties, receiverRecord) }
                    .map { serializeConsumerRecord(kafkaListenerProperties, it) }
                    .filter { receiverRecord -> filterMessage(kafkaListenerProperties, receiverRecord) }
                    .concatMapEager { receiverRecord ->
                        Flowable.defer { Flowable.just((kafkaListenerProperties.handler as SingleHandler).apply(receiverRecord)) }
                                .concatMapEager { result ->
                                    when (result) {
                                        is Mono<*> -> RxJava2Adapter.monoToCompletable(result).toSingleDefault(receiverRecord).toFlowable()
                                        is Completable -> result.toSingleDefault(1).toFlowable().map { receiverRecord }
                                        else -> Flowable.error<ReceiverRecord<Any, Any>>(IllegalStateException("Unknown return type ${result.javaClass}"))
                                    }
                                }
                                .doOnError { error -> log.error("Failed to process kafka message", error) }
                                .retryWhen { it.delay(kafkaListenerProperties.retryBackoffMillis, TimeUnit.MILLISECONDS) }
                    }
                    .toList()
        }
                .doOnError { error -> log.error("Failed to process batch", error) }
                .retryWhen { it.delay(kafkaListenerProperties.retryBackoffMillis, TimeUnit.MILLISECONDS) }
                .ignoreElement()
    }

    private fun <K, V> processBatch(
        kafkaListenerProperties: KafkaListenerProperties<K, V>,
        receiverRecords: MutableList<out ReceiverRecord<Bytes, Bytes>>
    ): Completable {
        return Single.defer {
            Flowable.fromIterable(receiverRecords)
                    .filter { receiverRecord -> preFilterMessage(kafkaListenerProperties, receiverRecord) }
                    .map { serializeConsumerRecord(kafkaListenerProperties, it) }
                    .filter { receiverRecord -> filterMessage(kafkaListenerProperties, receiverRecord) }
                    .toList()
                    .flatMap { records ->
                        if (records.isNotEmpty()) {
                            Single.defer { Single.just((kafkaListenerProperties.handler as BatchHandler).apply(records)) }
                                    .flatMap { result ->
                                        when (result) {
                                            is Mono<*> -> RxJava2Adapter.monoToCompletable(result).toSingleDefault(records)
                                            is Completable -> result.toSingleDefault(1).map { records }
                                            else -> Single.error<List<ReceiverRecord<Any, Any>>>(IllegalStateException("Unknown return type ${result.javaClass}"))
                                        }
                                    }
                                    .doOnError { error -> log.error("Failed to process kafka message", error) }
                                    .retryWhen { it.delay(kafkaListenerProperties.retryBackoffMillis, TimeUnit.MILLISECONDS) }
                        } else {
                            Single.just(emptyList())
                        }
                    }
        }
                .doOnError { error -> log.error("Failed to process batch", error) }
                .retryWhen { it.delay(kafkaListenerProperties.retryBackoffMillis, TimeUnit.MILLISECONDS) }
                .ignoreElement()
    }

    private fun <K, V> preFilterMessage(kafkaListenerProperties: KafkaListenerProperties<K, V>, receiverRecord: ReceiverRecord<Bytes, Bytes>): Boolean {
        val pass = kafkaListenerProperties.preFilter.apply(receiverRecord)
        if (!pass) {
            logMessage("Messages pre-filtered out", receiverRecord)
        }

        return pass
    }

    private fun <K, V> filterMessage(kafkaListenerProperties: KafkaListenerProperties<K, V>, receiverRecord: ReceiverRecord<K?, V?>): Boolean {
        val pass = kafkaListenerProperties.filter.apply(receiverRecord)
        if (!pass) {
            logMessage("Messages filtered out", receiverRecord)
        }

        return pass
    }

    private fun <K, V> serializeConsumerRecord(kafkaListenerProperties: KafkaListenerProperties<K, V>, originalReceiverRecord: ReceiverRecord<Bytes, Bytes>): ReceiverRecord<K?, V?> {
        val key = originalReceiverRecord.key()?.let { kafkaListenerProperties.keyDeserializer.deserialize(originalReceiverRecord.topic(), it.get()) }
        val value = originalReceiverRecord.value()?.let { kafkaListenerProperties.valueDeserializer.deserialize(originalReceiverRecord.topic(), it.get()) }

        return ReceiverRecord(
                ConsumerRecord(
                        originalReceiverRecord.topic(),
                        originalReceiverRecord.partition(),
                        originalReceiverRecord.offset(),
                        originalReceiverRecord.timestamp(),
                        originalReceiverRecord.timestampType(),
                        originalReceiverRecord.checksum(),
                        originalReceiverRecord.serializedKeySize(),
                        originalReceiverRecord.serializedValueSize(),
                        key,
                        value
                ),
                originalReceiverRecord.receiverOffset()
        )
    }

    private fun logMessage(prefix: String, receiverRecord: ReceiverRecord<*, *>) {
        log.debug(
                "$prefix.\nTopic: {}, Partition: {}, Offset: {}, Headers: {}",
                receiverRecord.receiverOffset().topicPartition().topic(),
                receiverRecord.receiverOffset().topicPartition().partition(),
                receiverRecord.receiverOffset().offset(),
                receiverRecord.headers().map { it.key() + ":" + String(it.value()) }
        )
    }

    private fun <K, V> mergeConfiguration(listener: KafkaListenerMeta<K, V>, kafkaProperties: KafkaProperties): KafkaListenerProperties<K, V> {
        val parallelism = MoreObjects.firstNonNull(listener.parallelism, kafkaProperties.consumer?.parallelism) ?: error("parallelism mandatory")
        return KafkaListenerProperties(
                handler = listener.handler,
                topics = listener.topics,
                keyClass = listener.keyClass,
                valueClass = listener.valueClass,
                keyDeserializer = listener.keyDeserializer,
                valueDeserializer = listener.valueDeserializer,
                preFilter = listener.preFilter,
                filter = listener.filter,
                groupId = MoreObjects.firstNonNull(listener.groupId, kafkaProperties.consumer?.groupId) ?: error("groupId mandatory"),
                batchSize = MoreObjects.firstNonNull(listener.batchSize, kafkaProperties.consumer?.batchSize) ?: error("batchSize mandatory"),
                parallelism = parallelism,
                maxPoolIntervalMillis = MoreObjects.firstNonNull(listener.maxPoolIntervalMillis, kafkaProperties.consumer?.maxPoolIntervalMillis) ?: error("maxPoolIntervalMillis mandatory"),
                batchWaitMillis = MoreObjects.firstNonNull(listener.batchWaitMillis, kafkaProperties.consumer?.batchWaitMillis) ?: error("batchWaitMillis mandatory"),
                retryBackoffMillis = MoreObjects.firstNonNull(listener.retryBackoffMillis, kafkaProperties.consumer?.retryBackoffMillis) ?: error("retryBackoffMillis mandatory"),
                partitionAssignmentStrategy = MoreObjects.firstNonNull(listener.partitionAssignmentStrategy, kafkaProperties.consumer?.partitionAssignmentStrategy) ?: error("partitionAssignmentStrategy mandatory"),
                autoOffsetReset = MoreObjects.firstNonNull(listener.autoOffsetReset, kafkaProperties.consumer?.autoOffsetReset) ?: error("autoOffsetReset mandatory"),
                scheduler = Schedulers.newParallel("kafka-consumer-${listener.topics}", parallelism)
        )
    }

    private data class KafkaListenerProperties<K, V>(
        val handler: Handler<*, *>,
        val topics: List<String>,
        val keyClass: Class<K>,
        val valueClass: Class<V>,
        val keyDeserializer: Deserializer<K>,
        val valueDeserializer: Deserializer<V>,
        val preFilter: Predicate<in ConsumerRecord<Bytes, Bytes>>,
        val filter: Predicate<in ConsumerRecord<in K, in V>>,
        val groupId: String,
        val batchSize: Int,
        val parallelism: Int,
        val maxPoolIntervalMillis: Int,
        val batchWaitMillis: Long,
        val retryBackoffMillis: Long,
        val partitionAssignmentStrategy: String,
        val autoOffsetReset: String,
        val scheduler: Scheduler
    )
}
