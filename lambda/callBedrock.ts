import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime"
import { DynamoDBClient } from "@aws-sdk/client-dynamodb"
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb"

const bedrockClient = new BedrockRuntimeClient({})
const ddbClient = new DynamoDBClient({})
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient)

const LOG_ENTRIES_TABLE = process.env.LOG_ENTRIES_TABLE!
const MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0" // Using Claude 3 Sonnet

// Prompt template for log classification and enrichment
const createPrompt = (logContent: string, isBatch: boolean) => {
  if (isBatch) {
    return `<task>
You are analyzing a batch of log entries. For each log entry, classify it as ERROR, WARNING, or INFO.
For errors, suggest troubleshooting steps. For warnings, provide guidance. If any INFO messages appear to be misclassified, reclassify them.

Respond with a JSON array where each element contains:
- "entry": the original log entry
- "classification": the classification (ERROR, WARNING, or INFO)
- "context": troubleshooting steps, guidance, or reclassification explanation

Log entries to analyze:
${logContent}
</task>`
  } else {
    return `<task>
Analyze this log entry and classify it as ERROR, WARNING, or INFO.
If it's an error, suggest troubleshooting steps. If it's a warning, provide guidance.
If it appears to be an INFO message but is actually an error or warning, reclassify it.

Respond with a JSON object containing:
- "classification": the classification (ERROR, WARNING, or INFO)
- "context": troubleshooting steps, guidance, or reclassification explanation

Log entry to analyze:
${logContent}
</task>`
  }
}

export const handler = async (event: any): Promise<any> => {
  try {
    const { logEntries } = event
    console.log(`Processing ${logEntries.length} log entries with Bedrock`)

    for (const entry of logEntries) {
      const { id, timestamp, content, isBatch, sourceFile } = entry

      // Create prompt for Bedrock
      const prompt = createPrompt(content, isBatch)

      // Call Bedrock
      const response = await bedrockClient.send(
        new InvokeModelCommand({
          modelId: MODEL_ID,
          contentType: "application/json",
          accept: "application/json",
          body: JSON.stringify({
            anthropic_version: "bedrock-2023-05-31",
            max_tokens: 4096,
            messages: [
              {
                role: "user",
                content: prompt,
              },
            ],
          }),
        }),
      )

      // Parse Bedrock response
      const responseBody = JSON.parse(new TextDecoder().decode(response.body))
      const aiResponse = responseBody.content[0].text

      let enrichedData
      try {
        // Parse the AI response (assuming it returned valid JSON)
        if (isBatch) {
          // For batch entries, we expect an array of classifications
          enrichedData = JSON.parse(aiResponse)
        } else {
          // For single entries, we expect a single classification object
          enrichedData = JSON.parse(aiResponse)
        }
      } catch (error) {
        console.error("Error parsing AI response:", error)
        enrichedData = {
          classification: "UNKNOWN",
          context: "Failed to parse AI response",
        }
      }

      // Store in DynamoDB
      if (isBatch) {
        // For batch entries, store each entry separately
        const batchEntries = content.split("\n")
        const batchSize = Math.min(batchEntries.length, enrichedData.length)

        for (let i = 0; i < batchSize; i++) {
          const entryData = enrichedData[i] || {
            classification: "UNKNOWN",
            context: "Missing AI classification",
          }

          await ddbDocClient.send(
            new PutCommand({
              TableName: LOG_ENTRIES_TABLE,
              Item: {
                id: `${id}-${i}`,
                timestamp,
                content: batchEntries[i],
                classification: entryData.classification,
                context: entryData.context,
                sourceFile,
              },
            }),
          )
        }
      } else {
        // For single entries, store directly
        await ddbDocClient.send(
          new PutCommand({
            TableName: LOG_ENTRIES_TABLE,
            Item: {
              id,
              timestamp,
              content,
              classification: enrichedData.classification,
              context: enrichedData.context,
              sourceFile,
            },
          }),
        )
      }
    }

    return { status: "success", processedEntries: logEntries.length }
  } catch (error) {
    console.error("Error calling Bedrock:", error)
    throw error
  }
}
