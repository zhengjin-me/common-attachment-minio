package me.zhengjin.common.attachment.autoconfig

import io.minio.BucketExistsArgs
import io.minio.MakeBucketArgs
import io.minio.MinioAsyncClient
import me.zhengjin.common.attachment.adapter.CustomMinioClient
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.AutoConfigureBefore
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@AutoConfigureBefore(AttachmentMinIOStorageAutoConfiguration::class)
@EnableConfigurationProperties(AttachmentMinIOStorageProperties::class)
@ConditionalOnProperty(prefix = "customize.common.storage", name = ["type"], havingValue = "minio")
class AttachmentMinioClientAutoConfiguration(
    private val attachmentMinioStorageProperties: AttachmentMinIOStorageProperties
) {

    private val logger = LoggerFactory.getLogger(AttachmentMinioClientAutoConfiguration::class.java)

    @Bean
    @ConditionalOnMissingBean
    fun minioClient(): CustomMinioClient {
        attachmentMinioStorageProperties.checkConfig()
        val client = MinioAsyncClient.builder()
            .endpoint(attachmentMinioStorageProperties.endpoint)
            .credentials(attachmentMinioStorageProperties.accessKey, attachmentMinioStorageProperties.secretKey)
            .build()
        val isExist = client.bucketExists(
            BucketExistsArgs
                .builder()
                .bucket(attachmentMinioStorageProperties.bucket!!)
                .build()
        ).get()
        if (isExist) {
            logger.info("Bucket already exists.")
        } else {
            client.makeBucket(
                MakeBucketArgs
                    .builder()
                    .bucket(attachmentMinioStorageProperties.bucket!!)
                    .build()
            )
        }
        return CustomMinioClient(client)
    }
}
