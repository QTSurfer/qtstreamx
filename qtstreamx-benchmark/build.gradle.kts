plugins {
    id("me.champeau.jmh") version "0.7.3"
}

dependencies {
    api(project(":qtstreamx-core"))
    api(project(":qtstreamx-exchange-binance"))

    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.9")

    // simdjson-java
    implementation("org.simdjson:simdjson-java:0.4.0")

    // fastjson2 (Alibaba)
    implementation("com.alibaba.fastjson2:fastjson2:2.0.64")

    // Gson (Google)
    implementation("com.google.code.gson:gson:2.14.0")

    // JMH
    jmh("org.openjdk.jmh:jmh-core:1.37")
    jmh("org.openjdk.jmh:jmh-generator-annprocess:1.37")
}

jmh {
    warmupIterations.set(3)
    iterations.set(5)
    fork.set(2)
    jvmArgs.set(listOf("--add-modules=jdk.incubator.vector"))
}
