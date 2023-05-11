package me.zhengjin.common.attachment.adapter

import com.google.common.collect.Multimap
import io.minio.ListPartsResponse
import io.minio.MinioAsyncClient
import io.minio.ObjectWriteResponse
import io.minio.errors.InsufficientDataException
import io.minio.errors.InternalException
import io.minio.errors.XmlParserException
import io.minio.messages.Part
import java.io.IOException
import java.security.InvalidKeyException
import java.security.NoSuchAlgorithmException
import java.util.concurrent.ExecutionException

class CustomMinioClient(client: MinioAsyncClient?) : MinioAsyncClient(client) {
    /**
     * 创建分片上传请求
     *
     * @param bucketName       存储桶
     * @param region           区域
     * @param objectName       对象名
     * @param headers          消息头
     * @param extraQueryParams 额外查询参数
     */
    @Throws(
        IOException::class,
        InvalidKeyException::class,
        NoSuchAlgorithmException::class,
        InsufficientDataException::class,
        InterruptedException::class,
        InternalException::class,
        XmlParserException::class,
        ExecutionException::class
    )
    fun initMultipartUpload(
        bucketName: String,
        region: String?,
        objectName: String,
        headers: Multimap<String, String>?,
        extraQueryParams: Multimap<String, String>?
    ): String {
        val future = super.createMultipartUploadAsync(bucketName, region, objectName, headers, extraQueryParams)
        return future.get().result().uploadId()
    }

    /**
     * 完成分片上传，执行合并文件
     *
     * @param bucketName       存储桶
     * @param region           区域
     * @param objectName       对象名
     * @param uploadId         上传ID
     * @param parts            分片
     * @param extraHeaders     额外消息头
     * @param extraQueryParams 额外查询参数
     */
    @Throws(
        IOException::class,
        InvalidKeyException::class,
        NoSuchAlgorithmException::class,
        InsufficientDataException::class,
        InternalException::class,
        XmlParserException::class,
        ExecutionException::class,
        InterruptedException::class
    )
    fun mergeMultipartUpload(
        bucketName: String,
        region: String?,
        objectName: String,
        uploadId: String,
        parts: Array<Part>,
        extraHeaders: Multimap<String, String>?,
        extraQueryParams: Multimap<String, String>?
    ): ObjectWriteResponse {
        val future = super.completeMultipartUploadAsync(
            bucketName,
            region,
            objectName,
            uploadId,
            parts,
            extraHeaders,
            extraQueryParams
        )
        return future.get()
    }

    /**
     * 查询分片数据
     *
     * @param bucketName       存储桶
     * @param region           区域
     * @param objectName       对象名
     * @param uploadId         上传ID
     * @param extraHeaders     额外消息头
     * @param extraQueryParams 额外查询参数
     */
    @Throws(
        NoSuchAlgorithmException::class,
        InsufficientDataException::class,
        IOException::class,
        InvalidKeyException::class,
        XmlParserException::class,
        InternalException::class,
        ExecutionException::class,
        InterruptedException::class
    )
    fun listMultipart(
        bucketName: String,
        region: String?,
        objectName: String,
        maxParts: Int,
        partNumberMarker: Int,
        uploadId: String,
        extraHeaders: Multimap<String, String>?,
        extraQueryParams: Multimap<String, String>?
    ): ListPartsResponse {
        val future = super.listPartsAsync(
            bucketName,
            region,
            objectName,
            maxParts,
            partNumberMarker,
            uploadId,
            extraHeaders,
            extraQueryParams
        )
        return future.get()
    }
}
