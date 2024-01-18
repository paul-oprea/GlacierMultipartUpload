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
python -m pip install botocore, bboto3
```

<!--custom.prerequisites.start-->
<!--custom.prerequisites.end-->

## Usage and parameters



### Instructions



At a command prompt run the following:

```
python GlacierMPU.py file_path vault_name [archive comment]
```
where:
- file_path is the _complete_ file path of the file; if it contains blanks, wrap it with double quotes
- vault_name is the name of the Glacier vault
- archive comment is optional and needs to be wrapped in double quotes (if it's ommitted then the archive is initialized with the string 'Archive for ' file_path) 

#### Example

```
python GlacierMPU.py "C:\Users\joebloggs\my big archive.zip" My_Vault "family photos and videos from the years in ZIP format"
```

Copyright Paul Tudor OPREA 2024.

Open Source Code.
