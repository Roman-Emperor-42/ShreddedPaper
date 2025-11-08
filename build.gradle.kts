import io.papermc.paperweight.util.*
import io.papermc.paperweight.util.constants.*
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.gradle.kotlin.dsl.maven

plugins {
    java
    `maven-publish`
    id("io.github.goooler.shadow") version "8.1.7"
    id("io.papermc.paperweight.patcher") version "2.0.0-beta.17"
}


val paperMavenPublicUrl = "https://repo.papermc.io/repository/maven-public/"

paperweight {
    upstreams.register("purpur") {
        repo = github("PurpurMC", "Purpur")
        ref = providers.gradleProperty("purpurRef")

        patchFile {
            path = "purpur-server/build.gradle.kts"
            outputFile = file("ShreddedPaper-Server/build.gradle.kts")
            patchFile = file("ShreddedPaper-Server/build.gradle.kts.patch")
        }
        patchFile {
            path = "purpur-api/build.gradle.kts"
            outputFile = file("ShreddedPaper-API/build.gradle.kts")
            patchFile = file("ShreddedPaper-API/build.gradle.kts.patch")
        }
        patchRepo("paperApi") {
            upstreamPath = "paper-api"
            patchesDir = file("ShreddedPaper-API/paper-patches")
            outputDir = file("paper-api")
        }
        patchDir("purpurApi") {
            upstreamPath = "purpur-api"
            excludes = listOf("build.gradle.kts", "build.gradle.kts.patch", "paper-patches")
            patchesDir = file("ShreddedPaper-API/purpur-patches")
            outputDir = file("purpur-api")
        }
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    repositories {
        mavenCentral()
        maven(paperMavenPublicUrl)
    }

    tasks.withType<AbstractArchiveTask>().configureEach {
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
    }
    tasks.withType<JavaCompile> {
        options.encoding = Charsets.UTF_8.name()
        options.release = 21
        options.isFork = true
        options.compilerArgs.addAll(listOf("-Xlint:-deprecation", "-Xlint:-removal"))
        options.forkOptions.memoryMaximumSize = "4g"
    }
    tasks.withType<Javadoc> {
        options.encoding = Charsets.UTF_8.name()
    }
    tasks.withType<ProcessResources> {
        filteringCharset = Charsets.UTF_8.name()
    }
    tasks.withType<Test> {
        testLogging {
            showStackTraces = true
            exceptionFormat = TestExceptionFormat.FULL
            events(TestLogEvent.STANDARD_OUT)
        }
    }

    extensions.configure<PublishingExtension> {
        repositories {
            maven("https://repo.purpurmc.org/snapshots") {
                name = "ShreddedPaper"
                credentials(PasswordCredentials::class)
            }
        }
    }
}

tasks.register("printMinecraftVersion") {
    doLast {
        println(providers.gradleProperty("mcVersion").get().trim())
    }
}

tasks.register("printShreddedPaperVersion") {
    doLast {
        println(project.version)
    }
}



subprojects {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    tasks.withType<JavaCompile> {
        options.encoding = Charsets.UTF_8.name()
        options.release = 21
        options.isFork = true
        options.compilerArgs.addAll(listOf("-Xlint:-deprecation", "-Xlint:-removal"))
    }
    tasks.withType<Javadoc> {
        options.encoding = Charsets.UTF_8.name()
    }
    tasks.withType<ProcessResources> {
        filteringCharset = Charsets.UTF_8.name()
    }
    tasks.withType<Test> {
        testLogging {
            showStackTraces = true
            exceptionFormat = TestExceptionFormat.FULL
            events(TestLogEvent.STANDARD_OUT)
        }
    }
    tasks.withType<AbstractArchiveTask>().configureEach {
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
    }
    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }
    extensions.configure<PublishingExtension> {
        repositories {
            maven("https://repo.purpurmc.org/snapshots") {
                name = "purpur"
                credentials(PasswordCredentials::class)
            }
        }
    }


    repositories {
        mavenCentral()
        maven("https://s01.oss.sonatype.org/content/repositories/snapshots/") // TODO - Adventure snapshot
        maven("https://repo.papermc.io/repository/maven-public/")
        maven("https://oss.sonatype.org/content/groups/public/")
        maven("https://ci.emc.gs/nexus/content/groups/aikar/")
        maven("https://repo.aikar.co/content/groups/aikar")
        maven("https://repo.md-5.net/content/repositories/releases/")
        maven("https://hub.spigotmc.org/nexus/content/groups/public/")
        maven("https://jitpack.io")
    }
}

tasks.register("purpurRefLatest") {
    // Update the paperRef in gradle.properties to be the latest commit
    val tempDir = layout.cacheDir("purpurRefLatest");
    val file = "gradle.properties";

    doFirst {
        data class GithubCommit(
            val sha: String
        )

        val purpurLatestCommitJson = layout.cache.resolve("purpurLatestCommit.json");
        download.get().download("https://api.github.com/repos/PurpurMC/Purpur/commits/ver/1.21.8", purpurLatestCommitJson);
        val purpurLatestCommit = gson.fromJson<paper.libs.com.google.gson.JsonObject>(purpurLatestCommitJson)["sha"].asString;

        copy {
                from(file)
                into(tempDir)
                filter { line: String ->
                    line.replace("purpurRef = .*".toRegex(), "purpurRef = $purpurLatestCommit")
                }
            }
    }

    doLast {
        copy {
            from(tempDir.file("gradle.properties"))
            into(project.file(file).parent)
        }
    }
}