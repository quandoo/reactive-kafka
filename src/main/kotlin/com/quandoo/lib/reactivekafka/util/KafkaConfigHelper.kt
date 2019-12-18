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
package com.quandoo.lib.reactivekafka.util

import com.quandoo.lib.reactivekafka.KafkaProperties
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
object KafkaConfigHelper {

    fun populateCommonConfig(kafkaProperties: KafkaProperties, config: Map<String, Any>): Map<String, Any> {
        val configCopy = HashMap<String, Any>(config)

        configCopy[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        kafkaProperties.securityProtocol?.let { configCopy[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = it }
        kafkaProperties.clientDnsLookup?.let { configCopy[CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG] = it }

        return configCopy
    }

    fun populateSslConfig(kafkaProperties: KafkaProperties, config: Map<String, Any>): Map<String, Any> {
        val configCopy = HashMap<String, Any>(config)

        if (kafkaProperties.ssl != null) {
            kafkaProperties.ssl!!.protocol?.let { configCopy[SslConfigs.SSL_PROTOCOL_CONFIG] = it }
            kafkaProperties.ssl!!.provider?.let { configCopy[SslConfigs.SSL_PROVIDER_CONFIG] = it }
            kafkaProperties.ssl!!.cypherSuites?.let { configCopy[SslConfigs.SSL_CIPHER_SUITES_CONFIG] = it }
            kafkaProperties.ssl!!.enabledProtocols?.let { configCopy[SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG] = it }
            kafkaProperties.ssl!!.keystoreType?.let { configCopy[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = it }
            kafkaProperties.ssl!!.keystoreLocation?.let { configCopy[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = it }
            kafkaProperties.ssl!!.keystorePassword?.let { configCopy[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = it }
            kafkaProperties.ssl!!.keyPassword?.let { configCopy[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = it }
            kafkaProperties.ssl!!.truststoreType?.let { configCopy[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = it }
            kafkaProperties.ssl!!.truststoreLocation?.let { configCopy[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = it }
            kafkaProperties.ssl!!.truststorePassword?.let { configCopy[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = it }
            kafkaProperties.ssl!!.keymanagerAlgorithm?.let { configCopy[SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG] = it }
            kafkaProperties.ssl!!.trustmanagerAlgorithm?.let { configCopy[SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG] = it }
            kafkaProperties.ssl!!.endpointIdentificationAlgorithm?.let { configCopy[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = it }
            kafkaProperties.ssl!!.secureRandomImplementation?.let { configCopy[SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG] = it }
        }

        return configCopy
    }

    fun populateSaslConfig(kafkaProperties: KafkaProperties, config: Map<String, Any>): Map<String, Any> {
        val configCopy = HashMap<String, Any>(config)

        if (kafkaProperties.sasl != null) {
            kafkaProperties.sasl!!.mechanism?.let { configCopy[SaslConfigs.SASL_MECHANISM] = it }
            kafkaProperties.sasl!!.jaas?.let { configCopy[SaslConfigs.SASL_JAAS_CONFIG] = it }
            kafkaProperties.sasl!!.clientCallbackHandlerClass?.let { configCopy[SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS] = it }
            kafkaProperties.sasl!!.loginCallbackHandlerClass?.let { configCopy[SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS] = it }
            kafkaProperties.sasl!!.loginClass?.let { configCopy[SaslConfigs.SASL_LOGIN_CLASS] = it }
            kafkaProperties.sasl!!.kerbosServiceName?.let { configCopy[SaslConfigs.SASL_KERBEROS_SERVICE_NAME] = it }
            kafkaProperties.sasl!!.kerbosKinitCmd?.let { configCopy[SaslConfigs.SASL_KERBEROS_KINIT_CMD] = it }
            kafkaProperties.sasl!!.kerbosTicketRenewWindowFactor?.let { configCopy[SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR] = it }
            kafkaProperties.sasl!!.kerbosTicketRenewJitter?.let { configCopy[SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER] = it }
            kafkaProperties.sasl!!.kerbosMinTimeBeforeRelogin?.let { configCopy[SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN] = it }
            kafkaProperties.sasl!!.loginRefreshWindowFactor?.let { configCopy[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR] = it }
            kafkaProperties.sasl!!.loginRefreshWindowJitter?.let { configCopy[SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER] = it }
            kafkaProperties.sasl!!.loginRefreshMinPeriodSeconds?.let { configCopy[SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS] = it }
            kafkaProperties.sasl!!.loginRefreshBufferSeconds?.let { configCopy[SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS] = it }
        }

        return configCopy
    }
}
