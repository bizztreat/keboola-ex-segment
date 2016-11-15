# Segment.io extract

## What It Does

This helps you to extract events stored in S3 bucket with Segment.io integration. Use following configuration:

<pre>
{
  "s3_bucket": "your-s3-bucket",
  "s3_prefix": "segment-logs/some-string",
  "#access_key": "ACCESS KEY",
  "#secret_access_key": "API SECRET",
  "region": "us-east-1"
}
</pre>


- `s3_prefix` is "folder in S3" where Segment saves events   
- `s3_bucket` is name of your bucket  
- `#access_key` is your S3 access key  
- `#secret_access_key` is your S3 secret access key  
- `region` is where your data is stored :)  

If you have any question contact support@bizztreat.com !

Cheers!
