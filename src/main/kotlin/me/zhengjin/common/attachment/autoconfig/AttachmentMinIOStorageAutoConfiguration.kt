package me.zhengjin.common.attachment.autoconfig

import io.minio.MinioClient
import me.zhengjin.common.attachment.adapter.AttachmentStorage
import me.zhengjin.common.attachment.adapter.MinioStorageAdapter
import me.zhengjin.common.attachment.repository.AttachmentRepository
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(AttachmentMinIOStorageProperties::class)
@ConditionalOnProperty(prefix = "customize.common.storage", name = ["type"], havingValue = "minio")
@Suppress("SpringJavaInjectionPointsAutowiringInspection")
class AttachmentMinIOStorageAutoConfiguration(
    private val attachmentRepository: AttachmentRepository,
    private val attachmentMinioStorageProperties: AttachmentMinIOStorageProperties,
) {
    private val logger = LoggerFactory.getLogger(AttachmentMinIOStorageAutoConfiguration::class.java)

    @Bean
    @ConditionalOnMissingBean
    fun attachmentStorage(minioClient: MinioClient?): AttachmentStorage {
        attachmentMinioStorageProperties.checkConfig()
        logger.info("attachment storage type: [minio]")
        return MinioStorageAdapter(
            attachmentRepository,
            attachmentMinioStorageProperties,
            minioClient!!
        )
    }
}
