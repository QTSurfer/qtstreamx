plugins {
    application
}

dependencies {
    api(project(":qtstreamx-core"))
    api(project(":qtstreamx-link"))
    implementation(project(":qtstreamx-ws-native"))
    implementation(project(":qtstreamx-chain-evm-rpc"))
    implementation(project(":qtstreamx-dex-core"))
    implementation(project(":qtstreamx-dex-uniswap-v2"))
    implementation(project(":qtstreamx-dex-uniswap-v3"))
    implementation(project(":qtstreamx-dex-discovery-uniswap"))
    implementation(project(":qtstreamx-dex-capture-csv"))
    implementation(project(":qtstreamx-market-aggregation"))
    implementation(project(":qtstreamx-exchange-binance"))
    implementation(project(":qtstreamx-exchange-bybit"))
    implementation(project(":qtstreamx-exchange-okx"))
    implementation(project(":qtstreamx-exchange-kraken"))
    implementation(project(":qtstreamx-exchange-bitget"))
    implementation(project(":qtstreamx-exchange-gateio"))
    implementation(project(":qtstreamx-exchange-htx"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.9")
    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")

    // Live capture IT (ExchangeCaptureIT) discovers the binance universe via its REST instrument
    // cache, which lives in its own discovery module (not a runtime dep of the canary app).
    testImplementation(project(":qtstreamx-discovery-binance"))
}

// Gate the live @Tag("it") capture smoke test out of the default `test` run (it opens real exchange
// WebSockets); opt in with `-Pit`.
tasks.test {
    useJUnitPlatform {
        if (!project.hasProperty("it")) {
            excludeTags("it")
        }
    }
    testLogging { showStandardStreams = true }
}

application {
    mainClass.set("com.qtsurfer.qtstreamx.canary.CaptureMain")
}

tasks.register<JavaExec>("capture") {
    group = "canary"
    description = "Capture WS frames + parsed records from an exchange for a given duration."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.qtsurfer.qtstreamx.canary.CaptureMain")
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "info")

    val exchange = providers.gradleProperty("exchange").orElse("")
    val mode = providers.gradleProperty("mode").orElse("spot")
    val symbols = providers.gradleProperty("symbols").orElse("BTC/USDT,ETH/USDT,SOL/USDT")
    val duration = providers.gradleProperty("duration").orElse("5")
    val out = providers.gradleProperty("out").orElse("/tmp/canary")
    val interval = providers.gradleProperty("interval").orElse("1m")

    doFirst {
        args = listOf(
            "--exchange", exchange.get(),
            "--mode", mode.get(),
            "--symbols", symbols.get(),
            "--duration", duration.get(),
            "--out", out.get(),
            "--interval", interval.get(),
        )
    }
}

tasks.register<JavaExec>("analyze") {
    group = "canary"
    description = "Compare parsed records of an exchange against Binance reference."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.qtsurfer.qtstreamx.canary.AnalyzeMain")
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "info")

    val reference = providers.gradleProperty("reference").orElse("")
    val target = providers.gradleProperty("target").orElse("")
    val report = providers.gradleProperty("report").orElse("/tmp/canary/report.md")

    doFirst {
        args = listOf(
            "--reference", reference.get(),
            "--target", target.get(),
            "--report", report.get(),
        )
    }
}

tasks.register<JavaExec>("captureUniswapV3") {
    group = "canary"
    description = "Capture confirmed Uniswap v3 trades, tickers, and event-time candles."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.qtsurfer.qtstreamx.canary.UniswapV3CaptureMain")
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "info")

    val network = providers.gradleProperty("network").orElse("eip155:1")
    val pools = providers.gradleProperty("pools").orElse("")
    val startBlock = providers.gradleProperty("startBlock").orElse("")
    val streamKey = providers.gradleProperty("streamKey").orElse("")
    val checkpointDir = providers.gradleProperty("checkpointDir").orElse("")
    val confirmations = providers.gradleProperty("confirmations").orElse("2")
    val overlapBlocks = providers.gradleProperty("overlapBlocks").orElse("2")
    val maxReplayBlocks = providers.gradleProperty("maxReplayBlocks").orElse("10000")
    val maxProviderLagBlocks = providers.gradleProperty("maxProviderLagBlocks").orElse("2")
    val durationSeconds = providers.gradleProperty("durationSeconds").orElse("300")
    val out = providers.gradleProperty("out").orElse("/tmp/canary/uniswap-v3")
    val intervalName = providers.gradleProperty("intervalName").orElse("1m")
    val intervalMicros = providers.gradleProperty("intervalMicros").orElse("60000000")
    val maxBlockRange = providers.gradleProperty("maxBlockRange").orElse("2000")
    val timeoutSeconds = providers.gradleProperty("timeoutSeconds").orElse("15")
    val retries = providers.gradleProperty("retries").orElse("3")

    doFirst {
        val descriptors = pools.get().split(";").filter { it.isNotBlank() }
        if (descriptors.isEmpty()) {
            throw GradleException("-Ppools must contain at least one pool descriptor")
        }
        if (startBlock.get().isBlank() || streamKey.get().isBlank()) {
            throw GradleException("-PstartBlock and -PstreamKey are required")
        }
        val captureArgs = mutableListOf(
            "--network", network.get(),
            "--start-block", startBlock.get(),
            "--stream-key", streamKey.get(),
            "--confirmations", confirmations.get(),
            "--overlap-blocks", overlapBlocks.get(),
            "--max-replay-blocks", maxReplayBlocks.get(),
            "--max-provider-lag-blocks", maxProviderLagBlocks.get(),
            "--duration-seconds", durationSeconds.get(),
            "--out", out.get(),
            "--interval-name", intervalName.get(),
            "--interval-micros", intervalMicros.get(),
            "--max-block-range", maxBlockRange.get(),
            "--timeout-seconds", timeoutSeconds.get(),
            "--retries", retries.get(),
        )
        if (checkpointDir.get().isNotBlank()) {
            captureArgs += listOf("--checkpoint-dir", checkpointDir.get())
        }
        descriptors.forEach { captureArgs += listOf("--pool", it) }
        args = captureArgs
    }
}

tasks.register<JavaExec>("captureUniswapV2") {
    group = "canary"
    description = "Capture confirmed Uniswap v2 trades, tickers, and event-time candles."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.qtsurfer.qtstreamx.canary.UniswapV2CaptureMain")
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "info")

    val network = providers.gradleProperty("network").orElse("eip155:1")
    val pairs = providers.gradleProperty("pairs").orElse("")
    val startBlock = providers.gradleProperty("startBlock").orElse("")
    val streamKey = providers.gradleProperty("streamKey").orElse("")
    val checkpointDir = providers.gradleProperty("checkpointDir").orElse("")
    val confirmations = providers.gradleProperty("confirmations").orElse("2")
    val overlapBlocks = providers.gradleProperty("overlapBlocks").orElse("2")
    val maxReplayBlocks = providers.gradleProperty("maxReplayBlocks").orElse("10000")
    val maxProviderLagBlocks = providers.gradleProperty("maxProviderLagBlocks").orElse("2")
    val durationSeconds = providers.gradleProperty("durationSeconds").orElse("300")
    val out = providers.gradleProperty("out").orElse("/tmp/canary/uniswap-v2")
    val intervalName = providers.gradleProperty("intervalName").orElse("1m")
    val intervalMicros = providers.gradleProperty("intervalMicros").orElse("60000000")
    val maxBlockRange = providers.gradleProperty("maxBlockRange").orElse("2000")
    val timeoutSeconds = providers.gradleProperty("timeoutSeconds").orElse("15")
    val retries = providers.gradleProperty("retries").orElse("3")

    doFirst {
        val descriptors = pairs.get().split(";").filter { it.isNotBlank() }
        if (descriptors.isEmpty()) {
            throw GradleException("-Ppairs must contain at least one pair descriptor")
        }
        if (startBlock.get().isBlank() || streamKey.get().isBlank()) {
            throw GradleException("-PstartBlock and -PstreamKey are required")
        }
        val captureArgs = mutableListOf(
            "--network", network.get(),
            "--start-block", startBlock.get(),
            "--stream-key", streamKey.get(),
            "--confirmations", confirmations.get(),
            "--overlap-blocks", overlapBlocks.get(),
            "--max-replay-blocks", maxReplayBlocks.get(),
            "--max-provider-lag-blocks", maxProviderLagBlocks.get(),
            "--duration-seconds", durationSeconds.get(),
            "--out", out.get(),
            "--interval-name", intervalName.get(),
            "--interval-micros", intervalMicros.get(),
            "--max-block-range", maxBlockRange.get(),
            "--timeout-seconds", timeoutSeconds.get(),
            "--retries", retries.get(),
        )
        if (checkpointDir.get().isNotBlank()) {
            captureArgs += listOf("--checkpoint-dir", checkpointDir.get())
        }
        descriptors.forEach { captureArgs += listOf("--pair", it) }
        args = captureArgs
    }
}

listOf("V2" to "v2", "V3" to "v3").forEach { (taskSuffix, version) ->
    tasks.register<JavaExec>("captureDiscoveredUniswap$taskSuffix") {
        group = "canary"
        description = "Discover selected Uniswap $version markets, then capture normalized data."
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("com.qtsurfer.qtstreamx.canary.UniswapDiscoveryCaptureMain")
        systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "info")

        val network = providers.gradleProperty("network").orElse("eip155:1")
        val factory = providers.gradleProperty("factory").orElse("")
        val factoryStartBlock = providers.gradleProperty("factoryStartBlock").orElse("")
        val discoverySafeHead = providers.gradleProperty("discoverySafeHead").orElse("")
        val quoteTokens = providers.gradleProperty("quoteTokens").orElse("")
        val baseTokens = providers.gradleProperty("baseTokens").orElse("")
        val activityLookbackBlocks = providers.gradleProperty("activityLookbackBlocks").orElse("0")
        val discoveryMaxScanBlocks = providers.gradleProperty("discoveryMaxScanBlocks").orElse("100000")
        val discoveryMaxMetadataCalls = providers.gradleProperty("discoveryMaxMetadataCalls").orElse("2000")
        val discoveryMaxCandidates = providers.gradleProperty("discoveryMaxCandidates").orElse("1000")
        val discoveryMaxOutput = providers.gradleProperty("discoveryMaxOutput").orElse("100")
        val captureStartBlock = providers.gradleProperty("captureStartBlock").orElse("")
        val streamKey = providers.gradleProperty("streamKey").orElse("")
        val checkpointDir = providers.gradleProperty("checkpointDir").orElse("")
        val confirmations = providers.gradleProperty("confirmations").orElse("2")
        val overlapBlocks = providers.gradleProperty("overlapBlocks").orElse("2")
        val maxReplayBlocks = providers.gradleProperty("maxReplayBlocks").orElse("10000")
        val maxProviderLagBlocks = providers.gradleProperty("maxProviderLagBlocks").orElse("2")
        val durationSeconds = providers.gradleProperty("durationSeconds").orElse("300")
        val out = providers.gradleProperty("out").orElse("/tmp/canary/uniswap-$version-discovery")
        val intervalName = providers.gradleProperty("intervalName").orElse("1s")
        val intervalMicros = providers.gradleProperty("intervalMicros").orElse("1000000")
        val maxBlockRange = providers.gradleProperty("maxBlockRange").orElse("2000")
        val timeoutSeconds = providers.gradleProperty("timeoutSeconds").orElse("15")
        val retries = providers.gradleProperty("retries").orElse("3")

        doFirst {
            if (factory.get().isBlank()) {
                throw GradleException("-Pfactory is required")
            }
            if (factoryStartBlock.get().isBlank()) {
                throw GradleException("-PfactoryStartBlock is required")
            }
            if (quoteTokens.get().isBlank()) {
                throw GradleException("-PquoteTokens is required")
            }
            if (baseTokens.get().isBlank()) {
                throw GradleException("-PbaseTokens is required")
            }
            if (discoverySafeHead.get().isBlank()
                || captureStartBlock.get().isBlank()
                || streamKey.get().isBlank()
            ) {
                throw GradleException(
                    "-PdiscoverySafeHead, -PcaptureStartBlock, and -PstreamKey are required",
                )
            }
            val captureArgs = mutableListOf(
                "--version", version,
                "--network", network.get(),
                "--factory", factory.get(),
                "--factory-start-block", factoryStartBlock.get(),
                "--quote-token", quoteTokens.get(),
                "--base-token", baseTokens.get(),
                "--activity-lookback-blocks", activityLookbackBlocks.get(),
                "--discovery-max-scan-blocks", discoveryMaxScanBlocks.get(),
                "--discovery-max-metadata-calls", discoveryMaxMetadataCalls.get(),
                "--discovery-max-candidates", discoveryMaxCandidates.get(),
                "--discovery-max-output", discoveryMaxOutput.get(),
                "--discovery-safe-head", discoverySafeHead.get(),
                "--capture-start-block", captureStartBlock.get(),
                "--stream-key", streamKey.get(),
                "--confirmations", confirmations.get(),
                "--overlap-blocks", overlapBlocks.get(),
                "--max-replay-blocks", maxReplayBlocks.get(),
                "--max-provider-lag-blocks", maxProviderLagBlocks.get(),
                "--duration-seconds", durationSeconds.get(),
                "--out", out.get(),
                "--interval-name", intervalName.get(),
                "--interval-micros", intervalMicros.get(),
                "--max-block-range", maxBlockRange.get(),
                "--timeout-seconds", timeoutSeconds.get(),
                "--retries", retries.get(),
            )
            if (checkpointDir.get().isNotBlank()) {
                captureArgs += listOf("--checkpoint-dir", checkpointDir.get())
            }
            args = captureArgs
        }
    }
}
