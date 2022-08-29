package me.zhengjin.common.attachment.autoconfig

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "customize.common.storage.minio")
class AttachmentMinIOStorageProperties {

    var bucket: String? = null
    var endpoint: String? = null
    var accessKey: String? = null
    var secretKey: String? = null

    fun checkConfig() {
        require(!bucket.isNullOrBlank()) { "请配置minio bucket" }
        require(!endpoint.isNullOrBlank()) { "请配置minio endpoint" }
        require(!accessKey.isNullOrBlank()) { "请配置minio accessKey" }
        require(!secretKey.isNullOrBlank()) { "请配置minio secretKey" }
    }
}
