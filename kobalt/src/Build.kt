import com.beust.kobalt.api.License
import com.beust.kobalt.api.Scm
import com.beust.kobalt.plugin.packaging.assemble
import com.beust.kobalt.plugin.publish.bintray
import com.beust.kobalt.project
import com.beust.kobalt.repos

val repos = repos()
val groupId = "com.antwerkz.gridfs"

object Versions {
    val version = "0.2-SNAPSHOT"
    val kotlin = "1.0.2"
    val driverVersion = "3.2.1"
}

val core = project {

    directory = "core"
    group = groupId
    name = "gridfs-fs-provider"
    artifactId = name
    version = Versions.version
    licenses = arrayListOf(License("Apache-2.0", "http://www.apache.org/licenses/LICENSE-2.0"))
    scm = Scm(url = "http://github.com/evanchooly/gridfs-fs-provider",
            connection = "https://github.com/evanchooly/gridfs-fs-provider.git",
            developerConnection = "git@github.com:evanchooly/gridfs-fs-provider.git")

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
        mavenJars {
        }
    }

    bintray {
        publish = true
    }
}

val tests = project(core) {

    directory = "tests"
    name = "provider-tests"
    group = groupId
    artifactId = name
    version = Versions.version

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
}
