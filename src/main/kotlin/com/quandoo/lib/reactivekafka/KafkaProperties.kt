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

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@ConfigurationProperties(prefix = "kafka")
class KafkaProperties {

    lateinit var bootstrapServers: String

    var securityProtocol: String? = null
    var clientDnsLookup: String? = null
    var consumer: KafkaConsumerProperties? = null
    var producer: KafkaProducerProperties? = null
    var ssl: SslProperties? = null
    var sasl: SaslProperties? = null

    class KafkaConsumerProperties {
        var groupId: String? = null
        var autoOffsetReset: String = "latest"
        var parallelism = 1
        var partitionAssignmentStrategy = "org.apache.kafka.clients.consumer.RangeAssignor"
        var batchSize = 10
        var maxPoolIntervalMillis = 300000
        var commitInterval = 200L
        var retryBackoffMillis = 100L
        var heartBeatIntervalMillis = 3000
        var sessionTimeoutMillis = 10000
        val commitBatchSize = batchSize
    }

    class KafkaProducerProperties {
        var maxInFlight: Int? = null
    }

    class SslProperties {
        var endpointIdentificationAlgorithm: String? = null
        var protocol: String? = null
        var enabledProtocols: String? = null
        var provider: String? = null
        var cypherSuites: String? = null
        var keystoreType: String? = null
        var keystoreLocation: String? = null
        var keystorePassword: String? = null
        var keyPassword: String? = null
        var truststoreType: String? = null
        var truststoreLocation: String? = null
        var truststorePassword: String? = null
        var keymanagerAlgorithm: String? = null
        var trustmanagerAlgorithm: String? = null
        var secureRandomImplementation: String? = null
    }

    class SaslProperties {
        var mechanism: String? = null
        var jaas: String? = null
        var clientCallbackHandlerClass: String? = null
        var loginCallbackHandlerClass: String? = null
        var loginClass: String? = null
        var kerbosServiceName: String? = null
        var kerbosKinitCmd: String? = null
        var kerbosTicketRenewWindowFactor: Double? = null
        var kerbosTicketRenewJitter: Double? = null
        var kerbosMinTimeBeforeRelogin: Long? = null
        var loginRefreshWindowFactor: Double? = null
        var loginRefreshWindowJitter: Double? = null
        var loginRefreshMinPeriodSeconds: Short? = null
        var loginRefreshBufferSeconds: Short? = null
    }
}
