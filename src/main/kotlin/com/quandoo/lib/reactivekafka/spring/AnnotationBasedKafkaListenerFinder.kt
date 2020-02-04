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
package com.quandoo.lib.reactivekafka.spring

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.daniel.shuy.kafka.jackson.serializer.KafkaJacksonDeserializer
import com.google.common.base.Predicate
import com.google.common.base.Predicates
import com.quandoo.lib.reactivekafka.consumer.BatchHandler
import com.quandoo.lib.reactivekafka.consumer.Handler
import com.quandoo.lib.reactivekafka.consumer.SingleHandler
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListener
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerFilter
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerFinder
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerMeta
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerPreFilter
import java.lang.invoke.ConstantCallSite
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import kotlin.reflect.KClass
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Bytes
import org.reflections.Reflections
import org.reflections.scanners.MethodAnnotationsScanner
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.beans.factory.config.EmbeddedValueResolver
import org.springframework.context.ApplicationContext

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
class AnnotationBasedKafkaListenerFinder(
    private val configurableBeanFactory: ConfigurableBeanFactory,
    private val applicationContext: ApplicationContext,
    private val objectMapper: ObjectMapper
) : KafkaListenerFinder<Any?, Any?> {

    @Suppress("UNCHECKED_CAST")
    override fun findListeners(): List<KafkaListenerMeta<String?, Any?>> {
        val embeddedValueResolver = EmbeddedValueResolver(configurableBeanFactory)
        val reflections = Reflections("", MethodAnnotationsScanner())
        val preFilterBeans = applicationContext.getBeansWithAnnotation(KafkaListenerPreFilter::class.java).values
        val filterBeans = applicationContext.getBeansWithAnnotation(KafkaListenerFilter::class.java).values
        val preFilterGroupIdMap = preFilterBeans
                .groupBy { embeddedValueResolver.resolveStringValue(it.javaClass.getAnnotation(KafkaListenerPreFilter::class.java).groupId)!! }

        val filterGroupIdMap = filterBeans
                .groupBy { embeddedValueResolver.resolveStringValue(it.javaClass.getAnnotation(KafkaListenerFilter::class.java).groupId)!! }
                .map { filters ->
                    filters.key to filters.value.groupBy {
                        val annotation = it.javaClass.getAnnotation(KafkaListenerFilter::class.java)
                        (String::class to annotation.valueClass)
                    }
                }.toMap()

        checkFilterImpl(preFilterBeans.map { it.javaClass }.toList())
        checkFilterImpl(filterBeans.map { it.javaClass }.toList())
        checkPreFilterUniqueness(preFilterGroupIdMap)
        checkFilterUniqueness(filterGroupIdMap)

        val lookup = MethodHandles.lookup()
        return reflections.getMethodsAnnotatedWith(KafkaListener::class.java)
                .map { method ->
                    val annotation = method.getAnnotation(KafkaListener::class.java)
                    val instance = configurableBeanFactory.getBean(method.declaringClass)
                    val preFilter = preFilterGroupIdMap[embeddedValueResolver.resolveStringValue(annotation.groupId)]?.let { it[0] as Predicate<ConsumerRecord<Bytes, Bytes>> }
                            ?: Predicates.alwaysTrue()
                    val filter = filterGroupIdMap[embeddedValueResolver.resolveStringValue(annotation.groupId)]?.get(String::class to annotation.valueType)?.let { it[0] as Predicate<ConsumerRecord<out Any?, out Any?>> }
                            ?: Predicates.alwaysTrue()
                    val implementationMethodHandle = lookup.unreflect(method)
                    val callSite = ConstantCallSite(implementationMethodHandle)
                    val invoker = callSite.dynamicInvoker()

                    KafkaListenerMeta(
                            handler = getHandler(method.parameterTypes, invoker, instance),
                            topics = annotation.topics.map { topic -> embeddedValueResolver.resolveStringValue(topic) as String },
                            keyClass = String::class.java as Class<String?>,
                            valueClass = annotation.valueType.java as Class<Any?>,
                            keyDeserializer = StringDeserializer() as Deserializer<String?>,
                            valueDeserializer = KafkaJacksonDeserializer(objectMapper, annotation.valueType.java) as Deserializer<Any?>,
                            preFilter = preFilter,
                            filter = filter,

                            groupId = StringUtils.trimToNull(embeddedValueResolver.resolveStringValue(annotation.groupId)),
                            batchSize = annotation.batchSize.let { if (it < 0) null else it },
                            parallelism = annotation.parallelism.let { if (it < 0) null else it },
                            maxPoolIntervalMillis = annotation.maxPoolIntervalMillis.let { if (it < 0) null else it },
                            batchWaitMillis = annotation.batchWaitMillis.let { if (it < 0) null else it },
                            retryBackoffMillis = annotation.retryBackoffMillis.let { if (it < 0) null else it },
                            partitionAssignmentStrategy = StringUtils.trimToNull(embeddedValueResolver.resolveStringValue(annotation.partitionAssignmentStrategy)),
                            autoOffsetReset = StringUtils.trimToNull(embeddedValueResolver.resolveStringValue(annotation.autoOffsetReset))
                    )
                }
    }

    private fun getHandler(listenerArgumentTypes: Array<Class<*>>, invoker: MethodHandle, instance: Any): Handler<*, *> {
        check(listenerArgumentTypes.size == 1) { "Listener has to define only one param" }
        check(ConsumerRecord::class.java.isAssignableFrom(listenerArgumentTypes[0]) || List::class.java.isAssignableFrom(listenerArgumentTypes[0])) { "Listeners have to have one single argument of type ConsumerRecord" }

        return when {
            ConsumerRecord::class.java.isAssignableFrom(listenerArgumentTypes[0]) -> {
                object : SingleHandler {
                    override fun apply(value: ConsumerRecord<*, *>): Any {
                        return invoker.invokeWithArguments(instance, value)
                    }
                }
            }
            List::class.java.isAssignableFrom(listenerArgumentTypes[0]) -> {
                object : BatchHandler {
                    override fun apply(value: List<ConsumerRecord<*, *>>): Any {
                        return invoker.invokeWithArguments(instance, value)
                    }
                }
            }
            else -> {
                throw IllegalStateException("Unknown handler type: ${listenerArgumentTypes[0]}")
            }
        }
    }

    private fun checkFilterImpl(filterClasses: List<Class<*>>) {
        filterClasses.forEach { check(Predicate::class.java.isAssignableFrom(it)) { "Filters have to implement com.google.common.base.Predicate" } }
    }

    private fun checkPreFilterUniqueness(preFilterBeans: Map<String, List<Any>>) {
        preFilterBeans.forEach {
            check(it.value.size == 1) { "Pre-Filters have to be unique per groupId" }
        }
    }

    private fun checkFilterUniqueness(filterBeans: Map<String, Map<Pair<KClass<String>, KClass<*>>, List<Any>>>) {
        filterBeans.forEach { perGroupId ->
            perGroupId.value.forEach { perValueClass ->
                check(perValueClass.value.size == 1) { "Filters have to be unique per groupId -> valueClass combination" }
            }
        }
    }
}
