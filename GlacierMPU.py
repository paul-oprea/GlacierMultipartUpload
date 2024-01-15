# Copyright (C) 2024 Paul Tudor OPREA
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

import os
import sys
import boto3

from GlacierChecksum import __MEGABYTE__, to_hex, compute_file_tree_hash, compute_bytearray_tree_hash

glacier = boto3.client('glacier') # create a client object to access Glacier as a global object since only one is needed

# Upload a file to Glacier in multiple parts
def body_upload(file_path, vault_name, upload_id, blocksize):
    counter = 0
    filesize = os.stat(file_path).st_size
    with open(file_path, 'rb') as f:
        while counter * blocksize <= filesize:
            print('Uploading segment\t' + str(counter + 1) + '\tout of \t' + str(int(filesize / blocksize) + 1))
            start = int(counter * blocksize)
            f.seek(start)
            block = f.read(blocksize)
            end = int(start + len(block) - 1)

            tree_hash = compute_bytearray_tree_hash(block)
            print(to_hex(tree_hash))
            upload_segment(block, start, end, to_hex(tree_hash), vault_name, upload_id)
            counter += 1
    f.close()
    return filesize


# invoke an API call to upload a multipart segment to Glacier
def upload_segment(block, start, end, checksum, vault, uploadId):
    segment_response = glacier.upload_multipart_part(
        vaultName=vault,
        uploadId=uploadId,
        checksum=checksum,
        range='bytes ' + str(start) + '-' + str(end) + '/*',
        body=block
    )
    print(segment_response)


# invoke a multipart initiation API call to Glacier
def initiate_multipart_upload(vault, archiveDescription, partsize, ):
    glacier = boto3.client('glacier')
    response = glacier.initiate_multipart_upload(
        vaultName=vault,
        archiveDescription=archiveDescription,
        partSize=str(partsize)
    )
    print(response)
    return response['uploadId']

# invoke a multipart upload finish API call to Glacier
def finish_multipart_upload(vault, uploadId, archiveSize, checksum):
    finish_response = glacier.complete_multipart_upload(
        vaultName=vault,
        uploadId=uploadId,
        archiveSize=str(archiveSize),
        checksum=checksum
    )
    return finish_response


if __name__ == '__main__':
    file_path = sys.argv[1]
    vault_name = sys.argv[2]
    if len(sys.argv) >= 3:
        comment_name = sys.argv[3]
    else:
        comment_name = 'Archive containing file ' + file_path
    blocksize = 128 * __MEGABYTE__
    upload_id = initiate_multipart_upload(vault_name, comment_name, blocksize)

    print('Successfully initiated upload ID:\t' + upload_id)
    try:
        filesize = body_upload(file_path, vault_name, upload_id, blocksize)
        response = finish_multipart_upload(vault_name, upload_id, filesize, compute_file_tree_hash(file_path))
        try:
            print('Completion attempt response HTTPStatusCode:\t' + str(response['ResponseMetadata']['HTTPStatusCode']))
            print('Successfully completed upload ID:\t' + upload_id)
        except:
            print('Could not parse the response metadata')
    except Exception as e:
        print(e)
        glacier.abort_multipart_upload(
            vaultName=vault_name,
            uploadId=upload_id
        )
