import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3"
import type { Readable } from "stream"
import { v4 as uuidv4 } from "uuid"

const s3Client = new S3Client({})

// Helper function to convert stream to string
async function streamToString(stream: Readable): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    stream.on("data", (chunk) => chunks.push(Buffer.from(chunk)))
    stream.on("error", reject)
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")))
  })
}

// Function to split log content into individual entries
function splitLogEntries(content: string): string[] {
  // This is a simplified approach - in a real system, you'd need more sophisticated parsing
  // based on your log format (JSON, syslog, custom format, etc.)

  // For this example, we'll assume each line is a separate log entry
  // Skip empty lines
  return content.split("\n").filter((line) => line.trim().length > 0)
}

// Function to determine if entries should be batched
function shouldBatchEntries(entries: string[]): boolean {
  // Simple heuristic: if average entry length is less than 100 characters, batch them
  const totalLength = entries.reduce((sum, entry) => sum + entry.length, 0)
  const averageLength = totalLength / entries.length
  return averageLength < 100
}

// Function to create batches of entries
function batchEntries(entries: string[], maxBatchSize = 10): string[][] {
  const batches: string[][] = []
  for (let i = 0; i < entries.length; i += maxBatchSize) {
    batches.push(entries.slice(i, i + maxBatchSize))
  }
  return batches
}

export const handler = async (event: any): Promise<any> => {
  try {
    const { bucket, key } = event
    console.log(`Processing log file: ${key} from bucket: ${bucket}`)

    // Get the file from S3
    const getObjectResponse = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key,
      }),
    )

    if (!getObjectResponse.Body) {
      throw new Error("Empty file body")
    }

    // Convert stream to string
    const content = await streamToString(getObjectResponse.Body as Readable)

    // Split into log entries
    const logEntries = splitLogEntries(content)
    console.log(`Found ${logEntries.length} log entries`)

    // Determine if we should batch entries
    const shouldBatch = shouldBatchEntries(logEntries)

    if (shouldBatch) {
      console.log("Entries are short, batching them for Bedrock processing")
      const batches = batchEntries(logEntries)

      // Prepare batched entries for Bedrock
      const entriesForBedrock = batches.map((batch) => ({
        id: uuidv4(),
        timestamp: new Date().toISOString(),
        content: batch.join("\n"),
        isBatch: true,
        batchSize: batch.length,
        sourceFile: key,
      }))

      return { logEntries: entriesForBedrock }
    } else {
      // Prepare individual entries for Bedrock
      const entriesForBedrock = logEntries.map((entry) => ({
        id: uuidv4(),
        timestamp: new Date().toISOString(),
        content: entry,
        isBatch: false,
        sourceFile: key,
      }))

      return { logEntries: entriesForBedrock }
    }
  } catch (error) {
    console.error("Error processing log file:", error)
    throw error
  }
}
