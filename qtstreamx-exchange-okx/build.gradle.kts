dependencies {
    api(project(":qtstreamx-core"))
    // InstrumentsCache interface. Same layering decision as exchange-bybit: keep discovery next
    // to the stream client until a dedicated qtstreamx-discovery-okx pulls its weight.
    api(project(":qtstreamx-link"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.9")
    implementation("org.slf4j:slf4j-api:2.0.18")
}
