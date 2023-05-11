package me.zhengjin.common.attachment.adapter

import cn.hutool.core.io.file.FileNameUtil
import cn.hutool.core.util.RandomUtil
import io.minio.GetObjectArgs
import io.minio.GetPresignedObjectUrlArgs
import io.minio.PutObjectArgs
import io.minio.StatObjectArgs
import io.minio.http.Method
import me.zhengjin.common.attachment.autoconfig.AttachmentMinIOStorageProperties
import me.zhengjin.common.attachment.controller.vo.AttachmentVO
import me.zhengjin.common.attachment.controller.vo.CompleteMultipartUploadRequestVO
import me.zhengjin.common.attachment.controller.vo.MultipartUploadCreateRequestVO
import me.zhengjin.common.attachment.controller.vo.MultipartUploadCreateResponseVO
import me.zhengjin.common.attachment.po.Attachment
import me.zhengjin.common.attachment.po.AttachmentModelHelper
import me.zhengjin.common.attachment.repository.AttachmentRepository
import java.io.InputStream
import java.util.concurrent.TimeUnit

open class MinioStorageAdapter(
    private val attachmentRepository: AttachmentRepository,
    private val minioStorageProperties: AttachmentMinIOStorageProperties,
    private val minioClient: CustomMinioClient,
) : AttachmentStorageAdapter(attachmentRepository) {

    override fun createMultipartUpload(vo: MultipartUploadCreateRequestVO): MultipartUploadCreateResponseVO {
        return try {
            vo.fileName = "${dateRuleDir()}/${FileNameUtil.mainName(vo.fileName)}-${
            RandomUtil.randomString(
                "abcdefghigklmnopqrstuvwxyzABCDEFGHIGKLMNOPQRSTUVWXYZ",
                5
            )
            }.${FileNameUtil.extName(vo.fileName)}"
            val response = MultipartUploadCreateResponseVO()
            response.uploadId = minioClient.initMultipartUpload(
                minioStorageProperties.bucket!!,
                null,
                vo.fileName!!,
                null,
                null
            )
            val reqParams: MutableMap<String, String> = HashMap()
            reqParams["uploadId"] = response.uploadId!!
            for (i in 0 until vo.chunkSize!!) {
                reqParams["partNumber"] = i.toString()
                val presignedObjectUrl = minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                        .method(Method.PUT)
                        .bucket(minioStorageProperties.bucket)
                        .`object`(vo.fileName)
                        .expiry(1, TimeUnit.HOURS)
                        .extraQueryParams(reqParams)
                        .build()
                )
                val uploadPartItem = MultipartUploadCreateResponseVO.UploadPartItem()
                uploadPartItem.partNo = i
                uploadPartItem.uploadUrl = presignedObjectUrl
                response.chunks.add(uploadPartItem)
            }
            response
        } catch (e: Exception) {
            throw java.lang.RuntimeException("分片初始化失败")
        }
    }

    override fun completeMultipartUpload(vo: CompleteMultipartUploadRequestVO): AttachmentVO {
        try {
            val listPartsResult = minioClient.listMultipart(
                minioStorageProperties.bucket!!,
                null,
                vo.fileName!!,
                vo.chunkSize!!,
                0,
                vo.uploadId!!,
                null,
                null
            ).result()
            minioClient.mergeMultipartUpload(
                minioStorageProperties.bucket!!,
                null,
                vo.fileName!!,
                vo.uploadId!!,
                listPartsResult.partList().toTypedArray(),
                null,
                null
            )
            val fileInfo = minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket(minioStorageProperties.bucket)
                    .`object`(vo.fileName)
                    .build()
            ).get()
            var attachment = Attachment()
            attachment.module = vo.module
            attachment.businessTypeCode = vo.businessTypeCode
            attachment.businessTypeName = vo.businessTypeName
            attachment.pkId = vo.pkId

            attachment.fileOriginName = FileNameUtil.getName(fileInfo.`object`())
            attachment.fileType = fileInfo.contentType()
            attachment.filePath = fileInfo.`object`()
            attachment.fileSize = fileInfo.size()
            attachment = attachmentRepository.save(attachment)

            return if (attachment.id != null) {
                val avo = AttachmentVO.transform(attachment)
                avo.url = super.share(attachment.id!!)
                avo
            } else {
                throw RuntimeException("file save failed!")
            }
        } catch (e: Exception) {
            throw java.lang.RuntimeException("分片合并失败")
        }
    }

    override fun share(attachmentId: Long): String {
        val args = GetPresignedObjectUrlArgs
            .builder()
            .method(Method.GET)
            .bucket(minioStorageProperties.bucket)
            .`object`(super.getAttachment(attachmentId).filePath)
            .expiry(1, TimeUnit.DAYS)
            .build()

        return minioClient.getPresignedObjectUrl(args)
    }

    /**
     * 获取文件流
     */
    override fun getAttachmentFileStream(attachment: Attachment): InputStream =
        minioClient.getObject(
            GetObjectArgs
                .builder()
                .bucket(minioStorageProperties.bucket)
                .`object`(attachment.filePath)
                .build()
        ).get()

    /**
     * 附件存储(最终方法)
     * @param file              文件流
     * @param module            业务模块
     * @param businessTypeCode  业务类型代码
     * @param businessTypeName  业务类型名称
     * @param pkId              业务键
     * @param originalFileName  原始文件名称
     * @param fileContentType   文件媒体类型
     * @param fileSize          文件大小(字节)
     * @param readOnly          附件是否只读 仅能预览 不能下载
     */
    override fun saveFiles(
        file: InputStream,
        module: String,
        businessTypeCode: String,
        businessTypeName: String,
        pkId: Long?,
        originalFileName: String,
        fileContentType: String,
        fileSize: Long,
        readOnly: Boolean
    ): AttachmentVO {
        AttachmentModelHelper.checkRegister(module, businessTypeCode)
        FileNameUtil.extName(originalFileName)
        val suffix = RandomUtil.randomString("abcdefghigklmnopqrstuvwxyzABCDEFGHIGKLMNOPQRSTUVWXYZ", 5)
        val mainName = FileNameUtil.mainName(originalFileName)
        val extName = FileNameUtil.extName(originalFileName)
        val dateDir = dateRuleDir()
        val fileName = "$mainName-$suffix.$extName"
        // 使用minio时, 统一使用/作为目录分隔符
        val storagePath = "$module/$dateDir/$fileName"

        val args = PutObjectArgs.builder()
            .bucket(minioStorageProperties.bucket)
            .`object`(storagePath)
            .stream(file, file.available().toLong(), -1)
            .contentType(fileContentType)
            .build()
        minioClient.putObject(args)

        val attachment = super.save(
            readOnly = readOnly,
            module = module,
            businessTypeCode = businessTypeCode,
            businessTypeName = businessTypeName,
            pkId = pkId,
            fileOriginName = fileName,
            fileType = fileContentType,
            filePath = storagePath,
            fileSize = fileSize,
        )
        attachment.url = super.share(attachment.id!!)
        return attachment
    }
}
