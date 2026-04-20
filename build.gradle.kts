plugins {
    `java-library`
    id("jacoco")
    id("com.diffplug.spotless") version "8.4.0"
    id("com.vanniktech.maven.publish") version "0.36.0"
}

group = "io.github.openlogiclab"
version = System.getenv("GITHUB_REF_NAME")?.removePrefix("v") ?: "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api("org.apache.kafka:kafka-clients:4.2.0")

    testImplementation(platform("org.junit:junit-bom:6.0.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.compileJava {
    doFirst {
        options.compilerArgs.addAll(listOf(
            "--module-path", classpath.asPath
        ))
        classpath = files()
    }
}

tasks.javadoc {
    doFirst {
        val mp = classpath.asPath
        (options as CoreJavadocOptions).apply {
            addMultilineStringsOption("-module-path").value = listOf(mp)
        }
        classpath = files()
    }
}

spotless {
    java {
        licenseHeaderFile("gradle/license-header.txt")
        importOrder()
        removeUnusedImports()
        googleJavaFormat()
        targetExclude("build/**")
    }
}

mavenPublishing {
    publishToMavenCentral(automaticRelease = true)
    signAllPublications()

    pom {
        name.set("kafka-pipeline")
        description.set("Lightweight Kafka consumer pipeline with concurrent processing, backpressure, and sliding-window offset management")
        inceptionYear.set("2026")
        url.set("https://github.com/openlogiclab/kafka-pipeline")
        licenses {
            license {
                name.set("Apache License 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("zachhuan")
                url.set("https://github.com/zachhuan")
            }
        }
        scm {
            url.set("https://github.com/openlogiclab/kafka-pipeline")
            connection.set("scm:git:git://github.com/openlogiclab/kafka-pipeline.git")
            developerConnection.set("scm:git:ssh://git@github.com/openlogiclab/kafka-pipeline.git")
        }
    }
}

tasks.test {
    useJUnitPlatform()
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required = true
        html.required = true
        csv.required = false
    }
}

tasks.jacocoTestCoverageVerification {
    violationRules {
        rule {
            limit {
                minimum = "0.70".toBigDecimal()
            }
        }
    }
}
