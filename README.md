# compress-and-backup

The main purpose of this repo is to compress and backup my files to the cloud.

## Considerations
- Cost
    - Backup
    - Store
        - S3 Standard
            - 0.023 USD / GB / month.  
        - S3 Glacier Deep Archive
            - 0.00099 USD / GB / month, min charge: 180 day, that means if the average data updating frequency is longer than 8 days (180/(0.023/0.00099)), we should consider using S3 Glacier Deep Archive.  
    - Restore
        - S3 Standard
            - Free
        - S3 Glacier Deep Archive
            - batch: 0.0025 USD / GB and would need to wait for several hours to begin the restoration.  
            - Introduce a local HDD backup to decrease the potential to perform a restoration from S3.

- Compression
    - To decrease S3 cost, we consolidate (Deep Archive: minimum eligible object size of 128 KB) and compress the data.
    - ZSTD is fast https://peazip.github.io/fast-compression-benchmark-brotli-zstandard.html  
    - Problems
        - The external consolidation process merges the files to a new tar file, that would take extra disk space.
        - zstd.exe doesn't support input/output path containing multi-character words(Japanese).  
    - Use the https://github.com/mcmilk/7-Zip-zstd instead.

- UX
    - Easy to use
    - Shouldn't be error-prone
    - Write some code in Python, it can do error handling better than bash does.  
        - Discover
            - Perform a crc32 checksum of the local files, and compare the values to the metadata of the S3 blobs to discover changes
        - Backup
            - If we have an existing zstd-7z archive file, check the crc32 in its metadata with the crc32 of the local files, to see if we need to overwrite the zstd-7z
            - Do a multipart upload
            - Add the crc32 checksum of the raw files to the metadata when uploading to the S3
        - Restore
            - WON'T IMPLEMENT, restore it manually.

- Diagnose
    https://aws.amazon.com/tw/blogs/aws-cloud-financial-management/discovering-and-deleting-incomplete-multipart-uploads-to-lower-amazon-s3-costs/

- Ref
    https://docs.aws.amazon.com/zh_tw/amazonglacier/latest/dev/uploading-an-archive-single-op-using-cli.html
