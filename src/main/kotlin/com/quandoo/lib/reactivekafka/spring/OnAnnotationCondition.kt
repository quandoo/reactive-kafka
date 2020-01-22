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

import org.reflections.Reflections
import org.reflections.scanners.MethodAnnotationsScanner
import org.springframework.boot.autoconfigure.condition.ConditionOutcome
import org.springframework.boot.autoconfigure.condition.SpringBootCondition
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.ConfigurationCondition
import org.springframework.core.type.AnnotatedTypeMetadata

/**
 * @author Emir Dizdarevic
 * @since 1.1.0
 */
class OnAnnotationCondition : SpringBootCondition(), ConfigurationCondition {

    private val reflections = Reflections("", MethodAnnotationsScanner())

    @Suppress("UNCHECKED_CAST")
    override fun getMatchOutcome(context: ConditionContext, metadata: AnnotatedTypeMetadata): ConditionOutcome {
        val allAnnotationAttributes = metadata.getAnnotationAttributes(ConditionalOnAnnotation::class.java.name)

        val beansFound = reflections.getMethodsAnnotatedWith(allAnnotationAttributes?.get("annotation") as? Class<Nothing>).count()
        return if (beansFound > 0) ConditionOutcome.match() else ConditionOutcome.noMatch("Didn't find any bean annotated with @ConditionalOnAnnotation")
    }

    override fun getConfigurationPhase(): ConfigurationCondition.ConfigurationPhase {
        return ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN
    }
}
