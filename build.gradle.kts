import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.50"
    kotlin("plugin.spring") version "1.3.50"
    kotlin("kapt") version "1.3.50"

    `maven-publish`
    id("org.jetbrains.dokka") version "0.9.16"
    id("net.researchgate.release") version "2.6.0"
    id("com.github.hierynomus.license") version "0.15.0"
}

repositories {
    jcenter()
    mavenCentral()
    mavenLocal()
}

group = "com.quandoo.lib"

val ossNexusUser: String by project
val ossNexusPassword: String by project
val ktlint by configurations.creating

dependencies {
    val jacksonVersion = "2.10.0"
    val springBootVersion = "2.2.1.RELEASE"
    val testcontainersVersion = "1.12.3"

    ktlint("com.pinterest:ktlint:0.35.0")
    implementation(kotlin("reflect"))
    implementation(kotlin("stdlib-jdk8"))

    // Utils
    implementation("org.reflections:reflections:0.9.9")
    implementation("com.fasterxml.jackson.core:jackson-core:$jacksonVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")

    api("io.reactivex.rxjava2:rxjava:2.2.14")
    api("io.projectreactor.addons:reactor-adapter:3.3.0.RELEASE")
    api("io.projectreactor.kafka:reactor-kafka:1.2.1.RELEASE")
    api("org.apache.kafka:kafka-clients:2.3.1")
    api("com.github.daniel-shuy:kafka-jackson-serializer:0.1.2") {
        exclude(module = "kafka-clients")
        exclude(module = "jackson-databind")
    }

    // Frameworks
    compileOnly("org.springframework.boot:spring-boot:$springBootVersion")
    compileOnly("org.springframework.boot:spring-boot-autoconfigure:$springBootVersion")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.28")

    // Test
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.5.2")
    testImplementation("org.assertj:assertj-core:3.14.0")
    testImplementation("org.springframework.boot:spring-boot-starter-test:$springBootVersion")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.awaitility:awaitility:4.0.1")
}

tasks {
    withType<JavaCompile> {
        sourceCompatibility = "1.8"
        targetCompatibility = "1.8"
    }

    withType<Test> {
        useJUnitPlatform()
    }

    withType<KotlinCompile> {
        kotlinOptions {
            freeCompilerArgs = listOf("-Xjsr305=strict")
            jvmTarget = "1.8"
        }
    }

    dokka {
        outputFormat = "html"
        outputDirectory = "$buildDir/javadoc"
        moduleName = rootProject.name
    }

    val ktlintVerify by creating(JavaExec::class) {
        group = "verification"
        description = "Check and Fix Kotlin code style."
        main = "com.pinterest.ktlint.Main"
        classpath = ktlint
        args("-F", "**/*.gradle.kts", "src/**/*.kt")
    }

    named("check") {
        dependsOn(ktlintVerify, named("license"))
    }

    named("afterReleaseBuild") {
        dependsOn(named("publish"))
    }
}

val dokkaJar by tasks.creating(Jar::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Assembles Kotlin docs with Dokka"
    archiveClassifier.set("javadoc")
    from(tasks.dokka)
    dependsOn(tasks.dokka)
}

val sourcesJar by tasks.creating(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.getByName("main").allSource)
}

publishing {
    publications {
        create<MavenPublication>("oss-nexus") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            // This is the main artifact
            from(components["java"])
            // We are adding documentation artifact
            artifact(dokkaJar)
            // And sources
            artifact(sourcesJar)
        }
    }

    repositories {
        maven {
            val releasesRepoUrl = "https://oss.sonatype.org/service/local/staging/deploy/maven2"
            val snapshotsRepoUrl = "https://oss.sonatype.org/content/repositories/snapshots"
            url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)
            credentials {
                username = System.getenv("OSS_MAVEN_REPO_USER") ?: ossNexusUser
                password = System.getenv("OSS_MAVEN_REPO_PASS") ?: ossNexusPassword
            }
        }
    }
}

license {
    header = file("HEADER")
    include("**/*.kt")
    strictCheck = true

    ext.set("year", "2019")
    ext.set("name", "Quandoo GbmH")
    ext.set("email", "account.oss@quandoo.com")
}
