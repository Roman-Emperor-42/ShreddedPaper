import java.util.Locale

pluginManagement {
    repositories {
        gradlePluginPortal()
        maven("https://papermc.io/repo/repository/maven-public/")
        // io.github.goooler.shadow
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "shreddedpaper"

for (name in listOf("ShreddedPaper-API", "ShreddedPaper-Server")) {
    val projName = name.lowercase(Locale.ENGLISH)
    include(projName)
    findProject(":$projName")!!.projectDir = file(name)
}
