# S3 Glacier Multipart Upload

## Overview

Shows how to use the AWS SDK for Python (Boto3) to work with Amazon S3 Glacier.

_S3 Glacier provides durable and extremely low-cost storage for infrequently used data with security features for data archiving and backup._

## ⚠ Important

* Running this code might result in charges to your AWS account. For more details, see [AWS Pricing](https://aws.amazon.com/pricing/) and [Free Tier](https://aws.amazon.com/free/).
* Running the tests might result in charges to your AWS account.
* We recommend that you grant your code least privilege. At most, grant only the minimum permissions required to perform the task. For more information, see [Grant least privilege](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html#grant-least-privilege).

## Code

### Prerequisites


Install botocore by running the following in a virtual environment:

```
python -m pip install botocore
```

<!--custom.prerequisites.start-->
<!--custom.prerequisites.end-->

## Usage and parameters



### Instructions



Start the example by running the following at a command prompt:

```
python GlacierMPU.py file_path vault_name [archive comment]
```


Copyright Paul Tudor OPREA 2024.

Open Source Code.