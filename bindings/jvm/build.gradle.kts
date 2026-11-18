plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(25))

tasks.withType<JavaCompile>().configureEach {
    options.release.set(25)
}

dependencies {
    api(project(":xtdb-api"))
    api(libs.arrow.adbc)
    api(libs.arrow.adbc.driver.manager)
    implementation(libs.clojure)
    implementation(libs.next.jdbc)
    implementation(libs.pgjdbc)
    implementation("org.apache.arrow:arrow-c-data:19.0.0")
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.jovial)
    testRuntimeOnly(libs.jsonista)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED")
    systemProperty("io.netty.noUnsafe", "false")
    systemProperty("io.netty.tryReflectionSetAccessible", "true")
    providers.systemProperty("xtdb.native.library").orNull?.let { systemProperty("xtdb.native.library", it) }
}
