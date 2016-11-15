# Segment.io extract

## What It Does

This helps you to extract events stored in S3 bucket with Segment.io integration. Use following configuration:

<code>
{
  "s3_bucket": "your-s3-bucket",
  "s3_prefix": "segment-logs/some-string",
  "#access_key": "ACCESS KEY",
  "#secret_access_key": "API SECRET",
  "region": "us-east-1"
}
</code>

- `s3_prefix` is "folder in S3" where Segment saves events.  
