import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { BedrockRuntimeClient, InvokeModelCommand } from '@aws-sdk/client-bedrock-runtime';

const s3 = new S3Client({});
const bedrock = new BedrockRuntimeClient({ region: process.env.REGION });

function splitLogFile(content: string, maxLen = 3000): string[] {
    const lines = content.split('\n');
    const chunks: string[] = [];
    let current = '';

    for (const line of lines) {
        const isNewEntry = /^(\[?(INFO|WARNING|ERROR|CRITICAL)\]?|[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2})/.test(line);
        if ((current.length + line.length > maxLen) && isNewEntry) {
            chunks.push(current);
            current = line + '\n';
        } else {
            current += line + '\n';
        }
    }
    if (current.trim()) chunks.push(current);
    return chunks;
}

export const handler = async (event: any) => {
    const bucket = process.env.LOG_BUCKET_NAME!;
    const key = event.key;

    const response = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const raw = await response.Body.transformToString();
    const parts = splitLogFile(raw);

    const results = [];

    for (const part of parts) {
        const input = {
            anthropic_version: "bedrock-2023-05-31",
            messages: [
                {
                    "role": "user",
                    "content": `
                        You are a system log analyzer.
        
                        Your task is to process a raw log file segment and output a structured JSON array of log entries.
                        
                        For each log entry in the input:
                        1. Detect the log boundary (each log line typically starts with a timestamp or a log level).
                        2. Extract the following fields when available: 
                           - \`timestamp\` (ISO format)
                           - \`logLevel\` (must be one of: INFO, WARNING, ERROR, CRITICAL)
                           - \`message\` (text content of the log)
                        3. Ensure that each entry has a \`logLevel\` – infer it if it's not explicitly mentioned. Analyze if the
                         log message may indicate silent fail. Correct the log level if you find it inappropriate. If you change
                         the label include a field called \`analysisExplanation\`. Make sure there is only one \`logLevel\` 
                         field in each log entry JSON object.
                        4. If \`logLevel\` is ERROR or CRITICAL:
                           - Add a field \`troubleshooting\` with a brief explanation of likely cause and actionable resolution
                            steps.
                        
                        Respond only with a valid JSON array where each object has this shape:
                       
                        {
                          "timestamp": "2025-05-21T10:23:45Z",
                          "logLevel": "ERROR",
                          "message": "Database connection failed",
                          "troubleshooting": "Check DB credentials and if the instance is reachable on the network."
                        }
                        ${part}`
                }
            ],
            max_tokens: 10000,
        };

        const command = new InvokeModelCommand({
            modelId: process.env.BEDROCK_MODEL_ID,
            contentType: 'application/json',
            accept: 'application/json',
            body: JSON.stringify(input),
        });

        const result = await bedrock.send(command);
        const decodedResponseBody = new TextDecoder().decode(result.body);
        const responseBody = JSON.parse(decodedResponseBody);
        results.push(responseBody);
    }

    return {
        message: `Processed ${results.length} log entries from ${key}`,
        results
    };
};
