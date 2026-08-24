dependencies {
    api(project(":qtstreamx-core"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.9")
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation("org.mockito:mockito-core:5.14.2")
    testImplementation("org.awaitility:awaitility:4.3.0")
    testImplementation("org.slf4j:slf4j-simple:2.0.16")
}
