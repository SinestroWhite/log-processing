import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3"
import { DynamoDBClient } from "@aws-sdk/client-dynamodb"
import { DynamoDBDocumentClient, GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb"

const s3Client = new S3Client({})
const ddbClient = new DynamoDBClient({})
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient)

const PROCESSED_FILES_TABLE = process.env.PROCESSED_FILES_TABLE!
const LOG_BUCKET = process.env.LOG_BUCKET!

export const handler = async (): Promise<any> => {
  try {
    console.log("Scanning S3 bucket for log files...")

    const filesToProcess = []
    let continuationToken: string | undefined = undefined

    do {
      const listResponse: any = await s3Client.send(
          new ListObjectsV2Command({
            Bucket: LOG_BUCKET,
            ContinuationToken: continuationToken,
          }),
      )

      const contents = listResponse.Contents ?? []
      for (const object of contents) {
        const fileKey = object.Key
        const eTag = object.ETag?.replace(/"/g, "")

        if (!fileKey || !eTag) {
          console.log('No fileKey and eTag')
          continue
        }

        const { Item: existingRecord } = await ddbDocClient.send(
            new GetCommand({
              TableName: PROCESSED_FILES_TABLE,
              Key: { fileKey },
            }),
        )

        const isModified = !existingRecord || existingRecord.eTag !== eTag

        if (isModified) {
          console.log(`File ${fileKey} is new or modified.`)

          await ddbDocClient.send(
              new PutCommand({
                TableName: PROCESSED_FILES_TABLE,
                Item: {
                  fileKey,
                  eTag,
                  lastProcessed: new Date().toISOString(),
                },
              }),
          )

          filesToProcess.push({ bucket: LOG_BUCKET, key: fileKey })
        } else {
          console.log(`File ${fileKey} has not changed.`)
        }
      }

      continuationToken = listResponse.IsTruncated ? listResponse.NextContinuationToken : undefined
    } while (continuationToken)

    console.log(`Found ${filesToProcess.length} files to process`)
    return { filesToProcess }
  } catch (error) {
    console.error("Error processing files:", error)
    throw error
  }
}
