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
for serial upload:
```
python GlacierMPU.py --source-path SOURCE_PATH --vault VAULT
                     [--comment COMMENT] [--upload-id UPLOAD_ID]
                     [--blocksize BLOCKSIZE] [--start START] [--end END]
                     [--finish-upload {True,False}]
```
or for parallel upload (potentially faster): notice it's called GlacierMPUP, as opposed to GlacierMPU for serial
```
python GlacierMPUP.py --source-path SOURCE_PATH --vault VAULT
                     [--comment COMMENT] [--upload-id UPLOAD_ID]
                     [--blocksize BLOCKSIZE] [--start START] [--end END]
                     [--threads THREADS] [--finish-upload {True,False}]
```
where:
- source-path is the _complete_ file path of the file; if it contains blanks, wrap it in double quotes;
- vault is the name of the Glacier vault; if it contains blanks, wrap it in double quotes;
- archive comment is optional and needs to be wrapped in double quotes (if it's omitted then the archive is initialized to the string 'Archive for ' source_path);
- upload-id is optional and is used to resume an upload that was interrupted; 
- blocksize is optional and is the size of the block in MB; it needs to be a power of 2; if it's omitted then it is set to 128MB;
- start is optional and is the block number from which to start the upload; if it's omitted then the upload starts from the beginning of the file;
- end is optional and is the block number at which to end the upload; if it's omitted then the upload ends at the end of the file;
- finish-upload is optional and is a boolean value indicating if to attempt to close the upload after all blocks have been uploaded (the only situation in which setting it to False is useful is if you want to only re-upload file chunks which may have failed in the middle of a previous run, and finish the process later); if it's omitted then the default value of True is used;
- threads is optional and is the number of threads to use for uploading; if it's omitted then the default value of 3 is used;

#### Example

```
python GlacierMPU.py --source-path "C:\Users\joebloggs\my big archive.zip" --vault My_Vault --comment "family photos and videos from the years in ZIP format"
```
### Application logic
#### Execution Flow
The application logic is as follows:
1. A new upload is initiated and the upload-id is created for the whole batch (except when the upload-id from a previous run is provided to be resumed);
2. The file is read into blocks of the specified size;
3. Each block is uploaded (in a separate thread when parallel upload is used);
4. If any of the block uploads fails:
   1. in the serial run, the entire process is stopped; if finish-upload is True, then the entire batch is aborted; if finish-upload is False, the batch is left open for a future run, and the upload-id can still be reused for about a day;
   2. in the parallel run, the upload continues with the next blocks. You can capture the IDs of failed threads from the output window and re-run the application with the same upload-id, but with the start and end parameters set to the block numbers of the failed threads. The application will then re-upload only the failed blocks;
5. If the batch upload is successful and all blocks have been uploaded, and finish-upload is True, then the upload is closed and the archive-id is returned;

#### Parallel upload
The optimal number of threads to use for uploading needs to be chosen depending on the network bandwidth and the number of cores of the CPU. If thread count is too low, the upload will be slower than hardware allows.
If it's too high, the upload will be slow as well, because the CPU will be busy switching between threads or may throw errors because the block upload API calls will time out.
As an example, on a 4-core CPU with 100Mbps outbound network bandwidth located on-prem, the optimal number of threads is around 3.

Copyright Paul Tudor OPREA 2024.

Open Source Code.
