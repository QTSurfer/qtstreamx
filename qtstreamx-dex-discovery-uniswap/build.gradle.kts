dependencies {
    api(project(":qtstreamx-core"))
    api(project(":qtstreamx-chain-evm-rpc"))
    api(project(":qtstreamx-dex-core"))
    api(project(":qtstreamx-dex-uniswap-v2"))
    api(project(":qtstreamx-dex-uniswap-v3"))
}

tasks.test {
    useJUnitPlatform {
        if (!project.hasProperty("it")) {
            excludeTags("it")
        }
    }
}
