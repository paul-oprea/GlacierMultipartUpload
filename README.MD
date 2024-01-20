# AWS Glacier Multipart Upload

## Overview

Automates uploading large files, (especially useful for those over 10 gigabytes), to AWS Glacier using the multipart upload API

## ⚠ Important

* Running this code might result in charges to your AWS account. For more details, see [AWS Pricing](https://aws.amazon.com/pricing/) and [Free Tier](https://aws.amazon.com/free/).
* Running the tests might result in charges to your AWS account.
* We recommend that you grant your code least privilege. At most, grant only the minimum permissions required to perform the task. For more information, see [Grant least privilege](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html#grant-least-privilege).

## Code

### Prerequisites
#### AWS CLI
The code assumes that AWS CLI has been installed and configured in the system

#### Python Libraries
Install the required libraries by running the following in a virtual environment:

```
python -m pip install botocore, boto3, argparse
```

<!--custom.prerequisites.start-->
<!--custom.prerequisites.end-->

## Usage and parameters



### Instructions



At a command prompt run the following:

```
python GlacierMPU.py --source-path SOURCE_PATH --vault VAULT
                     [--comment COMMENT] [--upload-id UPLOAD_ID]
                     [--blocksize BLOCKSIZE] [--start START] [--end END]
                     [--finish-upload {True,False}]
```
where:
- source-path is the _complete_ file path of the file; if it contains blanks, wrap it with double quotes;
- vault is the name of the Glacier vault; if it contains blanks, wrap it with double quotes;
- archive comment is optional and needs to be wrapped in double quotes (if it's omitted then the archive is initialized with the string 'Archive for ' source_path);
- upload-id is optional and is used to resume an upload that was interrupted; 
- blocksize is optional and is the size of the block in MB; it needs to be a power of 2; if it's omitted then the default value of 128MB is used;
- start is optional and is the block number from which to start the upload; if it's omitted then the default value of 0 is used;
- end is optional and is the block number at which to end the upload; if it's omitted then the default value of the number of blocks in the file is used;
- finish-upload is optional and is a boolean value indicating if to attempt to close the upload after all blocks have been uploaded (the only situation in which setting it to False is useful is if you want to only upload file chunks which may have failed in the middle, and finish the process later); if it's omitted then the default value of True is used;

#### Example

```
python GlacierMPU.py --source-path "C:\Users\joebloggs\my big archive.zip" --vault My_Vault --comment "family photos and videos from the years in ZIP format"
```

Copyright Paul Tudor OPREA 2024.

Open Source Code.
