Preambule: This work was inspired by the one from Steven Umbehocker (OSNEXUS) on RGW autotiering. Make sure to read his excellent article [here](https://ceph.io/en/news/blog/2024/auto-tiering-ceph-object-storage-part-1/) and to check his Github [repo](https://github.com/OSNEXUS/rgw-autotier)

Purpose: Highlight Ceph S3's support for dynamic placement and cost‑efficient, policy‑driven data retention.

Presentation at [slides/JoSy S3 - Strasbourg - Octobre 2025.pdf](https://github.com/FredNass/s3-dynamic-placement-and-archiving/blob/main/slides/JoSy%20S3%20-%20Strasbourg%20-%20Octobre%202025.pdf).

Scripting:

The [LUA script](https://github.com/FredNass/s3-dynamic-placement-and-archiving/blob/main/rgw_storageclass_rules.lua) on this repo was written from scratch to allow:
- using a default storage class for MPU uploads (mpu_default_class/mpu_force) as S3 object size can't be used as a rule criteria with MPU uploads
- using a default_class/default_force for non-MPU PUTs when no rules match
- selecting rules on the “most restrictive eligible match wins” using a specificity score for non-MPU PUTs
- using size units (SI and IEC)

Configuration:

```
# MPU PUTs settings
mpu_default_class=DEEP_ARCHIVE
mpu_force=true

# non-MPU PUTs settings
default_class=STANDARD_IA
default_force=false

# non-MPU PUTs rules
# STORAGECLASS;PATTERN;OP;BYTES;BUCKET;TENANT;OVERRIDE
#STANDARD_IA;%.pdf;*;0;*;*;true
#INTELLIGENT_TIERING;*;<;32768;bucket-logs;*;true
#ONEZONE_IA;%.eml;*;0;*;tenant-a;false
#GLACIER;%.iso;<;1073741824;media-bucket;*;true

STANDARD;*;<=;2MiB;*;*;true
DEEP_ARCHIVE;%.data;*;0B;*;*;true
```

Rules selection:

- Among all matched rules, only those that can effectively set the StorageClass (override=true or no client-provided StorageClass) are considered candidates.
- Each candidate gets a specificity score: +1 for a specific tenant, +1 for a specific bucket, +1 for a specific name pattern, +1 for a specific size operator. The rule with the highest score wins; on ties, the last rule in the file wins.
- If no candidate applies, the script falls back to default_class (if set), honoring default_force or preserving the client StorageClass if present.

Note regarding MPU (Multipart Uploads) vs non-MPU PUTs:

- non-MPU PUTs: Size thresholds use Request.ContentLength, and unit parsing supports both SI (KB/MB/GB/…) and IEC (KiB/MiB/GiB/…) with bare K/M/G/T/P treated as base 1024.
- MPU PUTs: at initiation, mpu_default_class is applied (forced if mpu_force=true); parts and completion are ignored by the rules.
