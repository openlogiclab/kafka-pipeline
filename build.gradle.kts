plugins {
    `java-library`
    id("jacoco")
    id("com.diffplug.spotless") version "8.4.0"
}

group = "io.github.openlogiclab"
version = "1.0-SNAPSHOT"

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

spotless {
    java {
        licenseHeaderFile("gradle/license-header.txt")
        importOrder()
        removeUnusedImports()
        googleJavaFormat()
        targetExclude("build/**")
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
