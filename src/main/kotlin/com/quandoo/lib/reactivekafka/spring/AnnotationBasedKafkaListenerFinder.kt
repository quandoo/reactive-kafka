/**
 *    Copyright (C) 2019 Quandoo GbmH (account.oss@quandoo.com)
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
) : KafkaListenerFinder<Any, Any> {

    @Suppress("UNCHECKED_CAST")
    override fun findListeners(): List<KafkaListenerMeta<String, Any>> {
        val embeddedValueResolver = EmbeddedValueResolver(configurableBeanFactory)
        val reflections = Reflections("", MethodAnnotationsScanner())
        val preFilterBeans = applicationContext.getBeansWithAnnotation(KafkaListenerPreFilter::class.java).values
        val filterBeans = applicationContext.getBeansWithAnnotation(KafkaListenerFilter::class.java).values
        val preFilterValueClassMap = preFilterBeans.map {
            val annotation = it.javaClass.getAnnotation(KafkaListenerPreFilter::class.java)
            (String::class to annotation.valueClass) to it
        }.toMap()
        val filterValueClassMap = filterBeans.map {
            val annotation = it.javaClass.getAnnotation(KafkaListenerFilter::class.java)
            (String::class to annotation.valueClass) to it
        }.toMap()

        checkFilters(preFilterBeans.map { it.javaClass }.toList())
        checkFilters(filterBeans.map { it.javaClass }.toList())

        val lookup = MethodHandles.lookup()
        return reflections.getMethodsAnnotatedWith(KafkaListener::class.java)
                .map { method ->
                    val annotation = method.getAnnotation(KafkaListener::class.java)
                    val instance = configurableBeanFactory.getBean(method.declaringClass)
                    val preFilter = preFilterValueClassMap[String::class to annotation.valueType]?.let { it as Predicate<ConsumerRecord<Bytes, Bytes>> } ?: Predicates.alwaysTrue()
                    val filter = filterValueClassMap[String::class to annotation.valueType]?.let { it as Predicate<ConsumerRecord<*, *>> } ?: Predicates.alwaysTrue()
                    val implementationMethodHandle = lookup.unreflect(method)
                    val callSite = ConstantCallSite(implementationMethodHandle)
                    val invoker = callSite.dynamicInvoker()

                    KafkaListenerMeta<String, Any>(
                            getHandler(method.parameterTypes, invoker, instance),
                            annotation.topics.map { topic -> embeddedValueResolver.resolveStringValue(topic) as String },
                            String::class.java,
                            annotation.valueType.java as Class<Any>,
                            StringDeserializer(),
                            KafkaJacksonDeserializer(objectMapper, annotation.valueType.java) as Deserializer<Any>,
                            preFilter,
                            filter
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

    private fun checkFilters(filterClasses: List<Class<*>>) {
        filterClasses.forEach { check(Predicate::class.java.isAssignableFrom(it)) { "Filters have to implement com.google.common.base.Predicate" } }
    }
}
