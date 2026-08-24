dependencies {
    api(project(":qtstreamx-core"))
    // InstrumentsCache interface. Same layering decision as bybit/okx — discovery alongside
    // the stream client until the discovery layer consolidates.
    api(project(":qtstreamx-link"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.9")
    implementation("org.slf4j:slf4j-api:2.0.16")
}
