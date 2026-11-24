plugins {
    `kotlin-dsl`
}

// The plugins the convention applies; their versions are set here, once, for every module.
dependencies {
    implementation("com.diffplug.spotless:spotless-plugin-gradle:8.11.0")
    implementation("com.vanniktech:gradle-maven-publish-plugin:0.37.0")
}
