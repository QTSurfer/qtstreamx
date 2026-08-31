plugins {
    application
    id("org.graalvm.buildtools.native") version "1.1.10"
}

dependencies {
    implementation(project(":qtstreamx-dex-capture-csv"))
    implementation(project(":qtstreamx-chain-evm-rpc"))
    implementation(project(":qtstreamx-dex-discovery-uniswap"))
    implementation(project(":qtstreamx-dex-uniswap-v2"))
    implementation(project(":qtstreamx-dex-uniswap-v3"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.9")
}

application {
    mainClass.set("com.qtsurfer.qtstreamx.dex.discovery.cli.DexDiscoveryCliMain")
    applicationName = "qtstreamx-dex-discovery"
}

graalvmNative {
    binaries {
        named("main") {
            imageName.set("qtstreamx-dex-discovery")
            mainClass.set(application.mainClass)
            // The root convention applies java-library to all modules. Without
            // this explicit value, Native Build Tools selects its shared-library
            // default instead of an executable for this application module.
            sharedLibrary.set(false)
            buildArgs.add("--enable-url-protocols=http,https")
        }
    }
}

tasks.register<Exec>("nativeSmoke") {
    group = "verification"
    description = "Runs deterministic endpoint-free smoke checks against the native DEX discovery CLI."
    dependsOn(tasks.named("nativeCompile"), tasks.named("nativeTest"))
    val isWindows = org.gradle.internal.os.OperatingSystem.current().isWindows
    // GitHub's Windows runners resolve a bare "bash" to the WSL launcher stub in
    // System32 before Git Bash, and that stub fails outright with no WSL
    // distribution installed. Pin Git for Windows' bash explicitly instead.
    executable(if (isWindows) "C:\\Program Files\\Git\\bin\\bash.exe" else "bash")
    val cliName = if (isWindows) "qtstreamx-dex-discovery.exe" else "qtstreamx-dex-discovery"
    args(
        layout.projectDirectory.file("src/native-smoke/native-smoke.sh").asFile.absolutePath,
        layout.buildDirectory.file("native/nativeCompile/$cliName").get().asFile.absolutePath
    )
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:deprecation")
}

tasks.named<Test>("test") {
    dependsOn(tasks.named("installDist"))
    // The application plugin's Windows launcher is the .bat script; the
    // extension-less script is a POSIX shell script that ProcessBuilder
    // cannot start directly on Windows.
    val launcherName = if (org.gradle.internal.os.OperatingSystem.current().isWindows) {
        "qtstreamx-dex-discovery.bat"
    } else {
        "qtstreamx-dex-discovery"
    }
    systemProperty(
        "qtstreamx.cli.executable",
        layout.buildDirectory.file(
            "install/qtstreamx-dex-discovery/bin/$launcherName"
        ).get().asFile.absolutePath
    )
}
