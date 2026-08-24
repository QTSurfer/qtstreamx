dependencies {
    api(project(":qtstreamx-core"))
    // InstrumentsCache interface. Keeping discovery alongside the stream client until a second
    // Bybit consumer makes a dedicated qtstreamx-discovery-bybit module worthwhile.
    api(project(":qtstreamx-link"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.9")
    implementation("org.slf4j:slf4j-api:2.0.18")
}
