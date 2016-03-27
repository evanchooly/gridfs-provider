import com.beust.kobalt.*
import com.beust.kobalt.plugin.packaging.*
import com.beust.kobalt.plugin.application.*
import com.beust.kobalt.plugin.kotlin.*

val repos = repos()

object Versions {
    val kotlin = "1.0.1"
    val driverVersion = "3.2.1"
}

val core = project {

    directory = "core"
    name = "gridfs-fs-provider"
    group = "com.antwerkz.gridfs"
    artifactId = name
    version = "0.1"

    sourceDirectories {
        path("src/main/kotlin")
    }

    sourceDirectoriesTest {
        path("src/test/kotlin")
    }

    dependencies {
        compile("org.slf4j:slf4j-api:1.7.6"/*, optional*/)
        compile("org.jetbrains.kotlin:kotlin-stdlib:${Versions.kotlin}")
        compile("org.mongodb:mongodb-driver:${Versions.driverVersion}")
    }

    dependenciesTest {
        compile("org.testng:testng:6.9.10")
    }

    assemble {
        jar {
        }
    }

    application {
        mainClass = "com.example.MainKt"
    }


}

val tests = project(core) {

    directory = "tests"
    name = "provider-tests"
    group = "com.antwerkz.gridfs"
    artifactId = name
    version = "0.1"

    sourceDirectories {
        path("src/main/kotlin")
    }

    sourceDirectoriesTest {
        path("src/test/kotlin")
    }

    dependencies {
        compile("org.slf4j:slf4j-api:1.7.6"/*, optional*/)
        compile("org.jetbrains.kotlin:kotlin-stdlib:${Versions.kotlin}")
        compile("org.mongodb:mongodb-driver:${Versions.driverVersion}")
        compile("org.testng:testng:6.9.10")
    }

    dependenciesTest {
        compile("org.testng:testng:6.9.10")
    }

    assemble {
        jar {
        }
    }
}
