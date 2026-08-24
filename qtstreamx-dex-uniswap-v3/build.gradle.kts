dependencies {
    api(project(":qtstreamx-core"))
    api(project(":qtstreamx-dex-core"))
    api(project(":qtstreamx-chain-evm-rpc"))
    testImplementation(project(":qtstreamx-ws-native"))
}

tasks.test {
    useJUnitPlatform {
        if (!project.hasProperty("it")) {
            excludeTags("it")
        }
    }
}
