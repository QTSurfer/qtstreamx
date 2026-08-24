dependencies {
    api(project(":qtstreamx-core"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.9")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation(project(":qtstreamx-ws-native"))
}

tasks.test {
    useJUnitPlatform {
        if (!project.hasProperty("it")) {
            excludeTags("it")
        }
    }
}
