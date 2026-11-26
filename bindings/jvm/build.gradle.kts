plugins {
    `java-library`
    alias(libs.plugins.clojurephant)
}

java.toolchain.languageVersion.set(JavaLanguageVersion.of(25))

val originalTestRuntime = rootProject.extensions.getByType<SourceSetContainer>().named("test").map { it.runtimeClasspath }

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
    testImplementation(files(originalTestRuntime).builtBy(rootProject.tasks.named("testClasses")))
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

tasks.test {
    useJUnitPlatform { excludeTags("integration") }
}

// The upstream test tree rust/conformance/upstream_tests.sh extracted; its sources shadow this repository's edited copies.
val corpusRootDir = rootProject.layout.projectDirectory
val corpusSources = providers.gradleProperty("corpusSources").map { corpusRootDir.file(it).asFile }
val corpusSourceDirs = corpusSources.map { root ->
    listOf("src/test/clojure", "src/test/resources", "src/testFixtures/clojure", "src/testFixtures/resources").map { root.resolve(it) }
}

tasks.register<JavaExec>("corpus") {
    description = "Runs every upstream XTDB test Var against the native engine and records every failure per Var."
    classpath = files(corpusSourceDirs.orElse(emptyList())) + sourceSets.test.get().runtimeClasspath
    doFirst {
        check(corpusSources.isPresent && corpusSources.get().resolve(".commit").isFile) {
            "corpus needs -PcorpusSources=<dir> naming a tree from rust/conformance/upstream_tests.sh; use rust/conformance/run_corpus.sh"
        }
    }
    mainClass.set("clojure.main")
    workingDir = rootProject.projectDir
    args("-m", "xtdb.rust.corpus")
    providers.gradleProperty("corpusNs").orNull?.let { args(it.split(",")) }
    javaLauncher.set(javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(25)) })
    jvmArgs("--enable-native-access=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-Xmx4g", "-XX:-OmitStackTraceInFastThrow")
    systemProperty("io.netty.noUnsafe", "false")
    systemProperty("io.netty.tryReflectionSetAccessible", "true")
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        corpusSources.orNull?.let { root ->
            listOf("-Dxtdb.corpus.root=${root.resolve("src/test/clojure").absolutePath}",
                   "-Dxtdb.corpus.fixtures-root=${root.resolve("src/testFixtures/clojure").absolutePath}")
        } ?: emptyList()
    })
    systemProperty("xtdb.corpus.out", providers.gradleProperty("corpusOut").orNull ?: layout.buildDirectory.file("corpus/results.jsonl").get().asFile.absolutePath)
    providers.gradleProperty("corpusTimeoutMs").orNull?.let { systemProperty("xtdb.corpus.timeout-ms", it) }
    providers.systemProperty("xtdb.native.library").orNull?.let { systemProperty("xtdb.native.library", it) }
}

tasks.register<Test>("integration-test") {
    val unitTests = tasks.test.get()
    testClassesDirs = unitTests.testClassesDirs
    classpath = unitTests.classpath
    useJUnitPlatform { includeTags("integration") }
}
