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
class KafkaConsumer {

    companion object {
        private val log = LoggerFactory.getLogger(KafkaConsumer::class.java)
    }

    private val kafkaProperties: KafkaProperties
    private val kafkaListenerMetas: List<KafkaListenerMeta<*, *>>
    private val schedulers: Map<KafkaListenerMeta<*, *>, Scheduler>

    constructor(
        kafkaProperties: KafkaProperties,
        kafkaListenerMetas: List<KafkaListenerMeta<*, *>>
    ) {
        checkKafkaProperties(kafkaProperties)
        checkListeners(kafkaListenerMetas)

        this.kafkaProperties = kafkaProperties
        this.kafkaListenerMetas = kafkaListenerMetas
        this.schedulers = kafkaListenerMetas.map { it to Schedulers.newParallel("kafka-consumer-${it.topics}", kafkaProperties.consumer!!.parallelism) }.toMap()
    }

    private fun checkKafkaProperties(kafkaProperties: KafkaProperties) {
        checkNotNull(kafkaProperties.consumer) { "Consumer properties have to be set" }
    }

    private fun checkListeners(listeners: List<KafkaListenerMeta<*, *>>): List<KafkaListenerMeta<*, *>> {
        check(listeners.map { it.valueClass }.size == listeners.map { it.valueClass }.toSet().size) { "Only one listener per Entity can be defined" }
        return listeners
    }

    private fun <K, V> createReceiverOptions(kafkaListenerMeta: KafkaListenerMeta<K, V>): ReceiverOptions<Bytes, Bytes> {
        val consumerProps = HashMap<String, Any>()
                .also {
                    it[ConsumerConfig.GROUP_ID_CONFIG] = kafkaProperties.consumer!!.groupId
                    it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = kafkaProperties.consumer!!.autoOffsetReset
                    it[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = kafkaProperties.consumer!!.batchSize.toInt()
                    it[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = kafkaProperties.consumer!!.maxPoolIntervalMillis
                    it[ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG] = kafkaProperties.consumer!!.partitionAssignmentStrategy
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
                .subscription(kafkaListenerMeta.topics)
                .schedulerSupplier { schedulers[kafkaListenerMeta] }
    }

    fun start() {
        for (i in 1..kafkaProperties.consumer!!.parallelism) {
            startConsumer()
        }
    }

    @SuppressWarnings("unchecked")
    private fun startConsumer() {
        kafkaListenerMetas.forEach { kafkaListenerMeta: KafkaListenerMeta<*, *> ->
            val receiverOptions = createReceiverOptions(kafkaListenerMeta)
            Flowable.defer {
                RxJava2Adapter.fluxToFlowable(
                        KafkaReceiver.create(receiverOptions).receive()
                                .bufferTimeout(kafkaProperties.consumer!!.batchSize.toInt(), Duration.ofMillis(kafkaProperties.consumer!!.batchWaitMillis), schedulers[kafkaListenerMeta]!!)
                )
            }
                    .flatMap(
                            { receiverRecords ->
                                when (kafkaListenerMeta.handler) {
                                    is SingleHandler -> {
                                        processSingle(kafkaListenerMeta, receiverRecords).toFlowable().map { receiverRecords }
                                    }
                                    is BatchHandler -> {
                                        processBatch(kafkaListenerMeta, receiverRecords).toFlowable().map { receiverRecords }
                                    }
                                    else -> {
                                        throw IllegalStateException("Unknown handler type: ${kafkaListenerMeta.handler.javaClass}")
                                    }
                                }
                                        .observeOn(io.reactivex.schedulers.Schedulers.from { r -> schedulers[kafkaListenerMeta]!!.schedule(r) })
                                        .concatMap(
                                                { receiverRecords ->
                                                    if (receiverRecords.isNotEmpty()) {
                                                        // All offsets need to be ackd
                                                        receiverRecords.forEach { it.receiverOffset().acknowledge() }

                                                        val lastReceiverRecordPerTopicPartition = receiverRecords.map { it.receiverOffset().topicPartition() to it }.toMap()
                                                        Flowable.fromIterable(lastReceiverRecordPerTopicPartition.values)
                                                                .flatMap { receiverRecord ->
                                                                    RxJava2Adapter.monoToCompletable(receiverRecord.receiverOffset().commit()).toSingle { receiverRecord }.toFlowable()
                                                                }
                                                                .toList()
                                                                .toFlowable()
                                                                .map { receiverRecords }
                                                    } else {
                                                        Flowable.just(receiverRecords)
                                                    }
                                                },
                                                1
                                        )
                            },
                            1
                    )
                    .retry()
                    .subscribeOn(io.reactivex.schedulers.Schedulers.from { r -> schedulers[kafkaListenerMeta]!!.schedule(r) }, true)
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
    }

    private fun <K, V> processSingle(
        kafkaListenerMeta: KafkaListenerMeta<K, V>,
        receiverRecords: MutableList<out ReceiverRecord<Bytes, Bytes>>
    ): Single<List<ReceiverRecord<*, *>>> {
        return Single.defer {
            Flowable.fromIterable(receiverRecords)
                    .filter { receiverRecord -> preFilterMessage(kafkaListenerMeta, receiverRecord) }
                    .map { serializeConsumerRecord(kafkaListenerMeta, it) }
                    .filter { receiverRecord -> filterMessage(kafkaListenerMeta, receiverRecord) }
                    .concatMapEager { receiverRecord ->
                        Flowable.defer { Flowable.just((kafkaListenerMeta.handler as SingleHandler).apply(receiverRecord)) }
                                .concatMapEager { result ->
                                    when (result) {
                                        is Mono<*> -> RxJava2Adapter.monoToCompletable(result).toSingleDefault(receiverRecord).toFlowable()
                                        is Completable -> result.toSingleDefault(1).toFlowable().map { receiverRecord }
                                        else -> Flowable.error<ReceiverRecord<Any, Any>>(IllegalStateException("Unknown return type ${result.javaClass}"))
                                    }
                                }
                                .doOnError { error -> log.error("Failed to process kafka message", error) }
                                .retryWhen { receiverRecord -> receiverRecord.delay(kafkaProperties.consumer!!.retryBackoffMillis, TimeUnit.MILLISECONDS) }
                    }
                    .toList()
        }
                .doOnError { error -> log.error("Failed to process batch", error) }
                .retryWhen { receiverRecord -> receiverRecord.delay(kafkaProperties.consumer!!.retryBackoffMillis, TimeUnit.MILLISECONDS) }
    }

    private fun <K, V> processBatch(
        kafkaListenerMeta: KafkaListenerMeta<K, V>,
        receiverRecords: MutableList<out ReceiverRecord<Bytes, Bytes>>
    ): Single<List<ReceiverRecord<*, *>>> {
        return Single.defer {
            Flowable.fromIterable(receiverRecords)
                    .filter { receiverRecord -> preFilterMessage(kafkaListenerMeta, receiverRecord) }
                    .map { serializeConsumerRecord(kafkaListenerMeta, it) }
                    .filter { receiverRecord -> filterMessage(kafkaListenerMeta, receiverRecord) }
                    .toList()
                    .flatMap { receiverRecords ->
                        if (receiverRecords.isNotEmpty()) {
                            Single.defer { Single.just((kafkaListenerMeta.handler as BatchHandler).apply(receiverRecords)) }
                                    .flatMap { result ->
                                        when (result) {
                                            is Mono<*> -> RxJava2Adapter.monoToCompletable(result).toSingleDefault(receiverRecords)
                                            is Completable -> result.toSingleDefault(1).map { receiverRecords }
                                            else -> Single.error<List<ReceiverRecord<Any, Any>>>(IllegalStateException("Unknown return type ${result.javaClass}"))
                                        }
                                    }
                                    .doOnError { error -> log.error("Failed to process kafka message", error) }
                                    .retryWhen { receiverRecords -> receiverRecords.delay(kafkaProperties.consumer!!.retryBackoffMillis, TimeUnit.MILLISECONDS) }
                        } else {
                            Single.just(emptyList())
                        }
                    }
        }
                .doOnError { error -> log.error("Failed to process batch", error) }
                .retryWhen { receiverRecord -> receiverRecord.delay(kafkaProperties.consumer!!.retryBackoffMillis, TimeUnit.MILLISECONDS) }
    }

    private fun <K, V> preFilterMessage(kafkaListenerMeta: KafkaListenerMeta<K, V>, receiverRecord: ReceiverRecord<Bytes, Bytes>): Boolean {
        val pass = kafkaListenerMeta.preFilter.apply(receiverRecord)
        if (!pass) {
            logMessage("Messages pre-filtered out", receiverRecord)
        }

        return pass
    }

    private fun <K, V> filterMessage(kafkaListenerMeta: KafkaListenerMeta<K, V>, receiverRecord: ReceiverRecord<*, *>): Boolean {
        val pass = kafkaListenerMeta.filter.apply(receiverRecord)
        if (!pass) {
            logMessage("Messages filtered out", receiverRecord)
        }

        return pass
    }

    private fun <K, V> serializeConsumerRecord(kafkaListenerMeta: KafkaListenerMeta<K, V>, originalReceiverRecord: ReceiverRecord<Bytes, Bytes>): ReceiverRecord<K?, V?> {
        val key = originalReceiverRecord.key()?.let { kafkaListenerMeta.keyDeserializer.deserialize(originalReceiverRecord.topic(), it.get()) }
        val value = originalReceiverRecord.value()?.let { kafkaListenerMeta.valueDeserializer.deserialize(originalReceiverRecord.topic(), it.get()) }

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
}
