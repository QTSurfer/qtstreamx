import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.external.javadoc.StandardJavadocDocletOptions
import org.gradle.api.plugins.JavaPluginExtension

plugins { base }

val libraryModules = setOf(
    "qtstreamx-core", "qtstreamx-codec-json", "qtstreamx-codec-msgpack",
    "qtstreamx-ws-native", "qtstreamx-ws-javaws", "qtstreamx-transport-nats",
    "qtstreamx-link", "qtstreamx-chain-evm-rpc", "qtstreamx-dex-core",
    "qtstreamx-dex-capture-csv", "qtstreamx-dex-uniswap-v2", "qtstreamx-dex-uniswap-v3",
    "qtstreamx-dex-discovery-uniswap", "qtstreamx-market-aggregation",
    "qtstreamx-discovery-binance", "qtstreamx-exchange-binance", "qtstreamx-exchange-bybit",
    "qtstreamx-exchange-okx", "qtstreamx-exchange-kraken", "qtstreamx-exchange-bitget",
    "qtstreamx-exchange-gateio", "qtstreamx-exchange-htx",
)

val projectVersion = providers.gradleProperty("version")
    .orElse(providers.environmentVariable("VERSION"))
    .orElse(providers.fileContents(layout.projectDirectory.file("VERSION")).asText.map(String::trim))
    .get()
val semver = Regex("\\d+\\.\\d+\\.\\d+(?:-[0-9A-Za-z.-]+)?")

// THIRD_PARTY_LICENSES.md is the single source of truth for the third-party set.
// Deriving the expected coordinates from it keeps a dependency bump to one
// inventory edit instead of several hand-synchronised copies that drift apart
// and turn the first automated bump into a red build.
fun inventoryCoordinates(section: String): Set<String> {
    val coordinate = Regex("`([\\w.-]+:[\\w.-]+:[\\w.+-]+)`")
    val found = sortedSetOf<String>()
    var inSection = false
    layout.projectDirectory.file("THIRD_PARTY_LICENSES.md").asFile.forEachLine { line ->
        if (line.startsWith("## ")) {
            inSection = line.removePrefix("## ").trim() == section
        } else if (inSection) {
            coordinate.findAll(line).forEach { found.add(it.groupValues[1]) }
        }
    }
    check(found.isNotEmpty()) { "License inventory section is empty or unparsable: $section" }
    return found
}

val publishedRuntimeDependencies = inventoryCoordinates("Published library runtime graph")
val cliDistributionJars = inventoryCoordinates("Runtime distribution set")
    .map { coordinate -> coordinate.split(":").let { "${it[1]}-${it[2]}.jar" } }
    .toSortedSet()

allprojects {
    group = "com.qtsurfer.qtstreamx"
    version = projectVersion
    repositories { mavenCentral() }
}

subprojects {
    apply(plugin = "java-library")

    extensions.configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_25
        targetCompatibility = JavaVersion.VERSION_25
        withSourcesJar()
        withJavadocJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(25)
    }
    tasks.withType<Test>().configureEach { useJUnitPlatform() }
    tasks.withType<Javadoc>().configureEach {
        options.encoding = "UTF-8"
        isFailOnError = true
        (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:all,-missing", true)
    }

    dependencies.add("testImplementation", "org.junit.jupiter:junit-jupiter:5.11.4")
    dependencies.add("testImplementation", "org.assertj:assertj-core:3.27.7")
    dependencies.add("testRuntimeOnly", "org.junit.platform:junit-platform-launcher")

    if (name in libraryModules) {
        apply(plugin = "maven-publish")
        extensions.configure<PublishingExtension> {
            publications {
                create<MavenPublication>("maven") {
                    from(components["java"])
                    pom {
                        name.set(project.name)
                        description.set("QTStreamX ${project.name.removePrefix("qtstreamx-")} library")
                        url.set("https://github.com/QTSurfer/qtstreamx")
                        licenses { license {
                            name.set("Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                            distribution.set("repo")
                        } }
                        developers { developer {
                            id.set("qtsurfer"); name.set("QTSurfer"); url.set("https://github.com/QTSurfer")
                        } }
                        // Public developer identity is QTSurfer; the licensor is
                        // the registered company that holds the copyright.
                        organization {
                            name.set("Wualabs LTD")
                            url.set("https://github.com/QTSurfer")
                        }
                        scm {
                            url.set("https://github.com/QTSurfer/qtstreamx")
                            connection.set("scm:git:https://github.com/QTSurfer/qtstreamx.git")
                            developerConnection.set("scm:git:ssh://git@github.com/QTSurfer/qtstreamx.git")
                        }
                        issueManagement {
                            system.set("GitHub"); url.set("https://github.com/QTSurfer/qtstreamx/issues")
                        }
                    }
                }
            }
        }
    }
}

tasks.register("verifyPublicationBoundaries") {
    group = "verification"
    description = "Checks that only the approved library modules publish Maven artifacts."
    doLast {
        val actual = subprojects.filter { it.plugins.hasPlugin("maven-publish") }.map { it.name }.toSet()
        check(actual == libraryModules) { "Publication boundary mismatch: expected $libraryModules, got $actual" }
        check(!project.plugins.hasPlugin("maven-publish")) { "Root aggregate must not publish" }
        subprojects.filter { it.name in libraryModules }.forEach { library ->
            val publication = library.extensions.getByType<PublishingExtension>().publications.getByName("maven")
                as MavenPublication
            check(publication.artifacts.any { it.classifier == "sources" }) { "${library.name} missing sources JAR" }
            check(publication.artifacts.any { it.classifier == "javadoc" }) { "${library.name} missing Javadoc JAR" }
        }
    }
}

tasks.register("verifyPublishedRuntimeLicenses") {
    group = "verification"
    description = "Checks the resolved external runtime graph of every JitPack library against its license inventory."
    doLast {
        val coordinates = libraryModules.flatMap { module ->
            project(":$module").configurations.getByName("runtimeClasspath")
                .resolvedConfiguration.resolvedArtifacts.map { artifact ->
                    val id = artifact.moduleVersion.id
                    "${id.group}:${id.name}:${id.version}"
                }
        }.filterNot { it.startsWith("${rootProject.group}:") }.toSortedSet()
        check(coordinates == publishedRuntimeDependencies) {
            "Published runtime graph does not match THIRD_PARTY_LICENSES.md: " +
                "expected $publishedRuntimeDependencies, got $coordinates"
        }
    }
}

tasks.register("verifyCliRuntimeLicenses") {
    group = "verification"
    description = "Checks the third-party JARs shipped in the CLI distribution against its license inventory."
    dependsOn(":qtstreamx-dex-discovery-cli:installDist")
    doLast {
        val libraries = project(":qtstreamx-dex-discovery-cli").layout.buildDirectory
            .dir("install/qtstreamx-dex-discovery/lib").get().asFile
        check(libraries.isDirectory) { "Missing built CLI runtime: $libraries" }
        val shipped = libraries.listFiles().orEmpty()
            .map { it.name }
            .filter { it.endsWith(".jar") && !it.startsWith("qtstreamx-") }
            .toSortedSet()
        check(shipped == cliDistributionJars) {
            "CLI distribution does not match THIRD_PARTY_LICENSES.md: " +
                "expected $cliDistributionJars, got $shipped"
        }
    }
}

tasks.register("verifyVersion") {
    group = "verification"
    doLast {
        check(semver.matches(projectVersion)) { "Invalid SemVer project version: $projectVersion" }
        val versionFile = layout.projectDirectory.file("VERSION").asFile.readText().trim()
        if (providers.gradleProperty("version").isPresent) {
            check(projectVersion == providers.gradleProperty("version").get()) { "-Pversion was not applied" }
        } else if (!providers.environmentVariable("VERSION").isPresent) {
            check(projectVersion == versionFile) { "VERSION file was not applied" }
        }
    }
}

tasks.register("verifyTagVersion") {
    group = "verification"
    doLast {
        val refType = providers.environmentVariable("GITHUB_REF_TYPE").orNull
        if (refType == "tag") {
            val tag = providers.environmentVariable("GITHUB_REF_NAME").orNull
            val versionFile = layout.projectDirectory.file("VERSION").asFile.readText().trim()
            check(tag != null && tag == versionFile) {
                "Git tag $tag must exactly match VERSION $versionFile"
            }
        }
    }
}

tasks.named("check") {
    dependsOn(
        "verifyPublicationBoundaries",
        "verifyPublishedRuntimeLicenses",
        "verifyCliRuntimeLicenses",
        "verifyVersion",
        "verifyTagVersion",
    )
}
tasks.named("build") {
    dependsOn(subprojects.map { it.tasks.named("build") })
}
