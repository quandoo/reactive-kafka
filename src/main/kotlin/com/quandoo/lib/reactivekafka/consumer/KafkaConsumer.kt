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
            check(entry.value.map { it.valueClass }.size == entry.value.map { it.valueClass }
                .toSet().size) { "Only one listener per groupID and Entity can be defined" }
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
                it[ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG] = kafkaListenerProperties.heartBeatIntervalMillis
                it[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = kafkaListenerProperties.sessionTimeoutMillis
                it[ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG] = kafkaListenerProperties.partitionAssignmentStrategy
            }
            .let { KafkaConfigHelper.populateCommonConfig(kafkaProperties, it) }
            .let { KafkaConfigHelper.populateSslConfig(kafkaProperties, it) }
            .let { KafkaConfigHelper.populateSaslConfig(kafkaProperties, it) }

        return ReceiverOptions.create<Bytes, Bytes>(consumerProps)
            .commitInterval(Duration.ofMillis(kafkaListenerProperties.commitInterval))
            .commitBatchSize(kafkaListenerProperties.batchSize)
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
        Flowable.defer { RxJava2Adapter.fluxToFlowable(receiver.receiveAutoAck()) }
            .map { RxJava2Adapter.fluxToFlowable(it) }
            .flatMap(
                { consumerRecords ->
                    when (kafkaListenerProperties.handler) {
                        is SingleHandler -> {
                            processSingle(kafkaListenerProperties, consumerRecords)
                        }
                        is BatchHandler -> {
                            processBatch(kafkaListenerProperties, consumerRecords)
                        }
                        else -> {
                            throw IllegalStateException("Unknown handler type: ${kafkaListenerProperties.handler.javaClass}")
                        }
                    }
                        .observeOn(io.reactivex.schedulers.Schedulers.from { r -> kafkaListenerProperties.scheduler.schedule(r) })
                },
                1
            )
            .observeOn(io.reactivex.schedulers.Schedulers.from { r -> kafkaListenerProperties.scheduler.schedule(r) })
            .doOnError { error -> log.error("Failed to kafka flux", error) }
            .retry()
            .subscribeOn(io.reactivex.schedulers.Schedulers.from { r -> kafkaListenerProperties.scheduler.schedule(r) }, true)
            .subscribe(
                object : DisposableSubscriber<List<ConsumerRecord<*, *>>>() {
                    override fun onStart() {
                        request(1)
                    }

                    override fun onNext(consumerRecords: List<ConsumerRecord<*, *>>) {
                        consumerRecords.forEach { logMessage("Message processed", it) }
                        request(1)
                    }

                    override fun onError(ex: Throwable) {
                        log.error("Consumer throw error", ex)
                    }

                    override fun onComplete() {
                        log.error("Consumer terminated")
                    }
                }
            )
    }

    private fun <K, V> processSingle(
        kafkaListenerProperties: KafkaListenerProperties<K, V>,
        consumerRecords: Flowable<ConsumerRecord<Bytes, Bytes>>
    ): Flowable<List<ConsumerRecord<*, *>>> {
        return consumerRecords
            .cache()
            .filter { consumerRecord -> preFilterMessage(kafkaListenerProperties, consumerRecord) }
            .map { serializeConsumerRecord(kafkaListenerProperties, it) }
            .filter { consumerRecord -> filterMessage(kafkaListenerProperties, consumerRecord) }
            .concatMapEager { consumerRecord ->
                Flowable.just((kafkaListenerProperties.handler as SingleHandler).apply(consumerRecord))
                    .concatMapEager { result ->
                        when (result) {
                            is Mono<*> -> RxJava2Adapter.monoToCompletable(result).toSingleDefault(consumerRecord).toFlowable()
                            is Completable -> result.toSingleDefault(1).toFlowable().map { consumerRecord }
                            else -> Flowable.error<ConsumerRecord<Any, Any>>(IllegalStateException("Unknown return type ${result.javaClass}"))
                        }
                    }
                    .doOnError { error -> log.error("Failed to process kafka message", error) }
                    .retryWhen { it.delay(kafkaListenerProperties.retryBackoffMillis, TimeUnit.MILLISECONDS) }
            }
            .toList()
            .toFlowable()
            .doOnError { error -> log.error("Failed to process batch", error) }
            .retryWhen { it.delay(kafkaListenerProperties.retryBackoffMillis, TimeUnit.MILLISECONDS) }
    }

    private fun <K, V> processBatch(
        kafkaListenerProperties: KafkaListenerProperties<K, V>,
        consumerRecords: Flowable<out ConsumerRecord<Bytes, Bytes>>
    ): Flowable<List<ConsumerRecord<*, *>>> {
        return consumerRecords
            .cache()
            .filter { consumerRecord -> preFilterMessage(kafkaListenerProperties, consumerRecord) }
            .map { serializeConsumerRecord(kafkaListenerProperties, it) }
            .filter { consumerRecord -> filterMessage(kafkaListenerProperties, consumerRecord) }
            .toList()
            .flatMap { innerConsumerRecords ->
                if (innerConsumerRecords.isNotEmpty()) {
                    Single.just((kafkaListenerProperties.handler as BatchHandler).apply(innerConsumerRecords))
                        .flatMap { result ->
                            when (result) {
                                is Mono<*> -> RxJava2Adapter.monoToCompletable(result).toSingleDefault(innerConsumerRecords)
                                is Completable -> result.toSingleDefault(1).map { innerConsumerRecords }
                                else -> Single.error<List<ConsumerRecord<Any, Any>>>(IllegalStateException("Unknown return type ${result.javaClass}"))
                            }
                        }
                        .doOnError { error -> log.error("Failed to process kafka message", error) }
                        .retryWhen { it.delay(kafkaListenerProperties.retryBackoffMillis, TimeUnit.MILLISECONDS) }
                } else {
                    Single.just(emptyList())
                }
            }
            .toFlowable()
            .doOnError { error -> log.error("Failed to process batch", error) }
            .retryWhen { it.delay(kafkaListenerProperties.retryBackoffMillis, TimeUnit.MILLISECONDS) }
    }

    private fun <K, V> preFilterMessage(kafkaListenerProperties: KafkaListenerProperties<K, V>, consumerRecord: ConsumerRecord<Bytes, Bytes>): Boolean {
        val pass = kafkaListenerProperties.preFilter.apply(consumerRecord)
        if (!pass) {
            logMessage("Messages pre-filtered out", consumerRecord)
        }

        return pass
    }

    private fun <K, V> filterMessage(kafkaListenerProperties: KafkaListenerProperties<K, V>, consumerRecord: ConsumerRecord<K?, V?>): Boolean {
        val pass = kafkaListenerProperties.filter.apply(consumerRecord)
        if (!pass) {
            logMessage("Messages filtered out", consumerRecord)
        }

        return pass
    }

    private fun <K, V> serializeConsumerRecord(
        kafkaListenerProperties: KafkaListenerProperties<K, V>,
        originalConsumerRecord: ConsumerRecord<Bytes, Bytes>
    ): ConsumerRecord<K?, V?> {
        val key = originalConsumerRecord.key()?.let { kafkaListenerProperties.keyDeserializer.deserialize(originalConsumerRecord.topic(), it.get()) }
        val value = originalConsumerRecord.value()?.let { kafkaListenerProperties.valueDeserializer.deserialize(originalConsumerRecord.topic(), it.get()) }

        return ConsumerRecord(
            originalConsumerRecord.topic(),
            originalConsumerRecord.partition(),
            originalConsumerRecord.offset(),
            originalConsumerRecord.timestamp(),
            originalConsumerRecord.timestampType(),
            originalConsumerRecord.checksum(),
            originalConsumerRecord.serializedKeySize(),
            originalConsumerRecord.serializedValueSize(),
            key,
            value
        )
    }

    private fun logMessage(prefix: String, consumerRecord: ConsumerRecord<*, *>) {
        log.debug(
            "$prefix.\nTopic: {}, Partition: {}, Offset: {}, Headers: {}",
            consumerRecord.topic(),
            consumerRecord.partition(),
            consumerRecord.offset(),
            consumerRecord.headers().map { it.key() + ":" + String(it.value()) }
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
            maxPoolIntervalMillis = MoreObjects.firstNonNull(listener.maxPoolIntervalMillis, kafkaProperties.consumer?.maxPoolIntervalMillis)
                ?: error("maxPoolIntervalMillis mandatory"),
            commitInterval = MoreObjects.firstNonNull(listener.commitInterval, kafkaProperties.consumer?.commitInterval) ?: error("commitInterval mandatory"),
            retryBackoffMillis = MoreObjects.firstNonNull(listener.retryBackoffMillis, kafkaProperties.consumer?.retryBackoffMillis)
                ?: error("retryBackoffMillis mandatory"),
            partitionAssignmentStrategy = MoreObjects.firstNonNull(listener.partitionAssignmentStrategy, kafkaProperties.consumer?.partitionAssignmentStrategy)
                ?: error("partitionAssignmentStrategy mandatory"),
            autoOffsetReset = MoreObjects.firstNonNull(listener.autoOffsetReset, kafkaProperties.consumer?.autoOffsetReset)
                ?: error("autoOffsetReset mandatory"),
            scheduler = Schedulers.newSingle("kafka-consumer-${listener.topics}"),
            heartBeatIntervalMillis = MoreObjects.firstNonNull(listener.heartBeatIntervalMillis, kafkaProperties.consumer?.heartBeatIntervalMillis)
                ?: error("heartBeatIntervalMillis mandatory"),
            sessionTimeoutMillis = MoreObjects.firstNonNull(listener.sessionTimeoutMillis, kafkaProperties.consumer?.sessionTimeoutMillis)
                ?: error("sessionTimeoutMillis mandatory"),
            commitBatchSize = MoreObjects.firstNonNull(listener.commitBatchSize, kafkaProperties.consumer?.commitBatchSize)
                ?: error("commitBatchSize mandatory")

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
        val commitInterval: Long,
        val retryBackoffMillis: Long,
        val partitionAssignmentStrategy: String,
        val heartBeatIntervalMillis: Int,
        val sessionTimeoutMillis: Int,
        val commitBatchSize: Int,
        val autoOffsetReset: String,
        val scheduler: Scheduler
    )
}
