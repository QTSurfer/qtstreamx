dependencies {
    api(project(":qtstreamx-core"))
    implementation("io.nats:jnats:2.26.2")
    implementation("org.slf4j:slf4j-api:2.0.18")
    constraints {
        implementation("org.bouncycastle:bcprov-lts8on:2.73.11")
    }
}
