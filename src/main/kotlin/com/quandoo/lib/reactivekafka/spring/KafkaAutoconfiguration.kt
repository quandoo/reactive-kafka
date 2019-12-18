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
import com.github.daniel.shuy.kafka.jackson.serializer.KafkaJacksonSerializer
import com.google.common.base.Predicate
import com.quandoo.lib.reactivekafka.KafkaProperties
import com.quandoo.lib.reactivekafka.consumer.KafkaConsumer
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerFinder
import com.quandoo.lib.reactivekafka.consumer.listener.KafkaListenerMeta
import com.quandoo.lib.reactivekafka.util.KafkaConfigHelper
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationListener
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.ContextRefreshedEvent
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@Configuration
@ConditionalOnClass(ObjectMapper::class)
@EnableConfigurationProperties(KafkaProperties::class)
class KafkaAutoconfiguration {

    @Autowired
    lateinit var configurableBeanFactory: ConfigurableBeanFactory

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var applicationConfiguration: ApplicationContext

    @Autowired(required = false)
    // Just to make sure that filters are loaded in the context before the autoconfig is executed
    lateinit var filters: List<Predicate<*>>

    @Bean
    @ConditionalOnMissingBean
    fun kafkaListenerFinder(): KafkaListenerFinder<*, *> {
        return AnnotationBasedKafkaListenerFinder(configurableBeanFactory, applicationConfiguration, objectMapper)
    }

    @Bean
    @ConditionalOnMissingBean
    @Suppress("UNCHECKED_CAST")
    @ConditionalOnProperty(prefix = "kafka.consumer", name = ["group-id"])
    fun kafkaConsumer(
        kafkaProperties: KafkaProperties,
        kafkaListenerFinder: KafkaListenerFinder<Any, Any>
    ): KafkaConsumer {
        return KafkaConsumer(
                kafkaProperties,
                kafkaListenerFinder.findListeners() as List<KafkaListenerMeta<Any, Any>>
        )
    }

    @Bean
    @ConditionalOnBean(KafkaConsumer::class)
    fun springStartupListener(kafkaConsumer: KafkaConsumer): SpringStartupListener {
        return SpringStartupListener(kafkaConsumer)
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kafka.producer", name = ["max-in-flight"])
    fun kafkaSender(
        kafkaProperties: KafkaProperties,
        objectMapper: ObjectMapper
    ): KafkaSender<String, Any> {
        val producerProps = HashMap<String, Any>()
                .let { KafkaConfigHelper.populateCommonConfig(kafkaProperties, it) }
                .let { KafkaConfigHelper.populateSslConfig(kafkaProperties, it) }
                .let { KafkaConfigHelper.populateSaslConfig(kafkaProperties, it) }

        val senderOptions = SenderOptions.create<String, Any>(producerProps)
                .withKeySerializer(StringSerializer())
                .withValueSerializer(KafkaJacksonSerializer<Any>(objectMapper))
                .maxInFlight(kafkaProperties.producer!!.maxInFlight.toInt())

        return KafkaSender.create<String, Any>(senderOptions)
    }
}

class SpringStartupListener(private val kafkaConsumer: KafkaConsumer) : ApplicationListener<ContextRefreshedEvent> {

    override fun onApplicationEvent(event: ContextRefreshedEvent) {
        kafkaConsumer.start()
    }
}
