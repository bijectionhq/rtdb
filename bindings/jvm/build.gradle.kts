plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(25))

dependencies {
    api(project(":xtdb-api"))
    api(libs.arrow.adbc)
    api(libs.arrow.adbc.driver.manager)
    implementation("org.apache.arrow:arrow-c-data:19.0.0")
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED")
    providers.systemProperty("xtdb.native.library").orNull?.let { systemProperty("xtdb.native.library", it) }
}
