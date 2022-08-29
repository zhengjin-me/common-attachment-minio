import java.io.ByteArrayOutputStream

plugins {
    // global version
    val kotlinVersion: String by System.getProperties()
    val dokkaVersion: String by System.getProperties()
    val ktlintVersion: String by System.getProperties()
    val nexusPublishVersion: String by System.getProperties()
    val springBootVersion: String by System.getProperties()
    val springDependencyManagementVersion: String by System.getProperties()

    idea
    `maven-publish`
    signing
    id("org.springframework.boot") version springBootVersion
    id("io.spring.dependency-management") version springDependencyManagementVersion
    id("org.jetbrains.dokka") version dokkaVersion
    id("org.jlleitschuh.gradle.ktlint") version ktlintVersion
    id("io.github.gradle-nexus.publish-plugin") version nexusPublishVersion
    kotlin("jvm") version kotlinVersion
    kotlin("plugin.spring") version kotlinVersion
}

// val releasesRepoUrl = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
// 使用nexusPublishing组件不能写完整路径
val releasesRepoUrl = uri("https://s01.oss.sonatype.org/service/local/")
val snapshotsRepoUrl = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")

val mavenUsername = (findProperty("MAVEN_CENTER_USERNAME") ?: System.getenv("MAVEN_CENTER_USERNAME")) as String?
val mavenPassword = (findProperty("MAVEN_CENTER_PASSWORD") ?: System.getenv("MAVEN_CENTER_PASSWORD")) as String?

val commonAttachmentVersion: String by project
val minioVersion: String by project
val okhttpVersion: String by project

group = "me.zhengjin"
// 使用最新的tag名称作为版本号
// version = { ext["latestTagVersion"] }

/**
 * 源码JDK版本
 */
java.sourceCompatibility = JavaVersion.VERSION_1_8
/**
 * 编译后字节码可运行环境的版本
 */
java.targetCompatibility = JavaVersion.VERSION_1_8

repositories {
    mavenLocal()
    mavenCentral()
//    maven {
//        url = releasesRepoUrl
//        credentials {
//            username = mavenUsername
//            password = mavenPassword
//        }
//    }
//    maven {
//        url = snapshotsRepoUrl
//        credentials {
//            username = mavenUsername
//            password = mavenPassword
//        }
//    }
}

dependencies {
    api("me.zhengjin:common-attachment:$commonAttachmentVersion")
    // minio
    api("io.minio:minio:$minioVersion")
    // minio 依赖于okhttp3, 为防止okhttp3版本过低导致异常, 在此处传递高版本信息
    api("com.squareup.okhttp3:okhttp:$okhttpVersion")
    api(kotlin("reflect"))
    api(kotlin("stdlib-jdk8"))
    testCompileOnly("org.springframework.boot:spring-boot-starter-test") {
        exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
    }
}

java {
    withJavadocJar()
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            val projectUrl: String by project
            val projectAuthor: String by project
            val projectAuthorEmail: String by project
            val licenseName: String by project
            val licenseUrl: String by project

            from(components["java"])
            pom {
                name.set(project.name)
                description.set(project.name)
                url.set(projectUrl)
                licenses {
                    license {
                        name.set(licenseName)
                        url.set(licenseUrl)
                    }
                }
                developers {
                    developer {
                        id.set(projectAuthor)
                        name.set(projectAuthor)
                        email.set(projectAuthorEmail)
                    }
                }
                scm {
                    url.set(projectUrl)
                }
                versionMapping {
                    usage("java-api") {
                        fromResolutionOf("runtimeClasspath")
                    }
                    usage("java-runtime") {
                        fromResolutionResult()
                    }
                }
            }
        }
    }
// 普通私有库发布
//    repositories {
//        maven {
//            url = if (isReleaseVersion) releasesRepoUrl else snapshotsRepoUrl
//            credentials {
//                username = mavenUsername
//                password = mavenPassword
//            }
//        }
//    }
}

// maven center 发布, 发布后自动释放
nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(releasesRepoUrl)
            snapshotRepositoryUrl.set(snapshotsRepoUrl)
            username.set(mavenUsername)
            password.set(mavenPassword)
        }
    }
}

signing {
    val signingKey: String? by project
    val signingPassword: String? by project
    useInMemoryPgpKeys(signingKey, signingPassword)
    sign(publishing.publications["mavenJava"])
}

tasks {
    register("getLatestTagVersion") {
        ext["latestTagVersionNumber"] = ByteArrayOutputStream().use {
            try {
                exec {
                    commandLine("git", "rev-list", "--tags", "--max-count=1")
                    standardOutput = it
                }
            } catch (e: Exception) {
                logger.error("Failed to get latest tag version number: [${e.message}]")
                return@use "unknown"
            }
            return@use it.toString().trim()
        }

        ext["latestTagVersion"] = ByteArrayOutputStream().use {
            try {
                exec {
                    commandLine("git", "describe", "--tags", ext["latestTagVersionNumber"])
                    standardOutput = it
                }
            } catch (e: Exception) {
                logger.error("Failed to get latest tag version: [${e.message}]")
                return@use "unknown"
            }
            val tagName = it.toString().trim()
            return@use Regex("^v?(?<version>\\d+\\.\\d+.\\d+(?:-SNAPSHOT|-snapshot)?)\$").matchEntire(tagName)?.groups?.get("version")?.value
                ?: throw IllegalStateException("Failed to get latest tag version, tagName: [$tagName]")
        }
        project.version = ext["latestTagVersion"]!!
        ext["isReleaseVersion"] = !version.toString().endsWith("SNAPSHOT", true)
        println("当前构建产物: [${project.group}:${project.name}:${project.version}]")
    }

    build {
        // 执行build之前 先获取版本号
        dependsOn("getLatestTagVersion")
    }

    publish {
        // 执行publish之前 先获取版本号
        dependsOn("getLatestTagVersion")
    }

    bootJar {
        enabled = false
    }

    jar {
        enabled = true
        classifier = ""
    }

    /**
     * 定义那些注解修饰的类自动开放
     */
    allOpen {
        annotations(
            "javax.persistence.Entity",
            "javax.persistence.MappedSuperclass",
            "javax.persistence.Embeddable"
        )
    }

    test {
        useJUnitPlatform()
    }

    /**
     * kotlin编译
     */
    compileKotlin {
        kotlinOptions {
            freeCompilerArgs = listOf("-Xjsr305=strict")
            jvmTarget = "1.8"
        }
    }

    withType<Sign>().configureEach {
        onlyIf { ext["isReleaseVersion"] as Boolean }
    }
}
