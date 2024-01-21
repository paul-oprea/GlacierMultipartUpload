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
import argparse

from GlacierChecksum import __MEGABYTE__, to_hex, compute_file_tree_hash, compute_bytearray_tree_hash

glacier = boto3.client(
    'glacier')  # create a client object to access Glacier as a global object since only one is needed

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Upload a file to Glacier in multiple parts
def body_upload(file_path, vault_name, upload_id, blocksize, start_block=0, end_block=-1):
    filesize = os.stat(file_path).st_size
    if end_block == -1:
        end_block = int(filesize / blocksize)
    with open(file_path, 'rb') as f:
        while start_block * blocksize <= min(filesize, end_block * blocksize):
            print('Uploading segment\t' + str(start_block + 1) + '\tout of \t' + str(int(filesize / blocksize) + 1))
            start = int(start_block * blocksize)
            f.seek(start)
            block = f.read(blocksize)
            end = int(start + len(block) - 1)

            tree_hash = compute_bytearray_tree_hash(block)
            print(to_hex(tree_hash))
            upload_segment(block, start, end, to_hex(tree_hash), vault_name, upload_id)
            start_block += 1
    f.close()
    return filesize


# invoke an API call to upload a multipart segment to Glacier
def upload_segment(block, start, end, checksum, vault, uploadId):
    print(f"\nStarting upload for segment {start}-{end}")
    segment_response = glacier.upload_multipart_part(
        vaultName=vault,
        uploadId=uploadId,
        checksum=checksum,
        range='bytes ' + str(start) + '-' + str(end) + '/*',
        body=block
    )
    print(bcolors.OKBLUE + str(segment_response) + bcolors.ENDC)


# invoke a multipart initiation API call to Glacier
def initiate_multipart_upload(vault, archiveDescription, partsize, ):
    glacier = boto3.client('glacier')
    response = glacier.initiate_multipart_upload(
        vaultName=vault,
        archiveDescription=archiveDescription,
        partSize=str(partsize)
    )
    print(bcolors.OKBLUE + str(response) + bcolors.ENDC)
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

def abort_multipart_upload(vault, uploadId):
    glacier.abort_multipart_upload(
        vaultName=vault,
        uploadId=uploadId
    )

def initialize_context(parser):
    # Define arguments
    parser.add_argument("--source-path", type=str, help="Full path to the source file", required=True)
    parser.add_argument("--vault", type=str, help="The name of the vault", required=True)
    parser.add_argument("--comment", type=str,
                        help="Optional comment to be added to the archive, default is Archive for + file path")
    parser.add_argument("--upload-id", type=str, help="The upload id of an already started upload session")
    parser.add_argument("--blocksize", type=int, help="block size in MB (only valid powers of 2)")
    parser.add_argument("--start", type=int, help="the start block")
    parser.add_argument("--end", type=int, help="the end block")
    parser.add_argument("--threads", type=int, help="Maximum number of threads to use for upload")
    parser.add_argument("--finish-upload", type=str, choices=['True', 'False'],
                        help="Whether to attempt to close the upload session at the end of the upload (default True) ")

    args = parser.parse_args()

    file_path = args.source_path
    vault_name = args.vault
    if args.comment is not None:
        comment_name = args.comment
    else:
        comment_name = 'Archive containing file ' + file_path
    if args.blocksize is not None:
        blocksize = args.blocksize * __MEGABYTE__
    else:
        blocksize = 128 * __MEGABYTE__

    if args.upload_id is not None:
        upload_id = args.upload_id
    else:
        upload_id = initiate_multipart_upload(vault_name, comment_name, blocksize)
        print('Successfully initiated upload ID:\t' + upload_id)
    if args.start is not None:
        start_block = args.start
    else:
        start_block = 0
    if args.end is not None:
        end_block = args.end
    else:
        end_block = -1

    if args.finish_upload is None:
        finish_upload = True
    else:
        finish_upload = True if args.finish_upload == 'True' else False

    if args.threads is not None:
        threads = args.threads
    else:
        threads = -1

    return file_path, vault_name, upload_id, blocksize, start_block, end_block, finish_upload, threads


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Glacier Multipart Uploader")
    file_path, vault_name, upload_id, blocksize, start_block, end_block, finish_upload, nonusedthreads = initialize_context(parser)
    try:
        filesize = body_upload(file_path, vault_name, upload_id, blocksize, start_block)
        if finish_upload:
            response = finish_multipart_upload(vault_name, upload_id, filesize, compute_file_tree_hash(file_path))
            try:
                print(bcolors.OKBLUE + 'Completion attempt response HTTPStatusCode:\t' + str(response['ResponseMetadata']['HTTPStatusCode']) + bcolors.ENDC)
                print(bcolors.OKGREEN + f'Successfully completed upload ID:\t{upload_id}' + bcolors.ENDC)
            except:
                print(bcolors.WARNING + 'Could not parse the response metadata' + bcolors.ENDC)
    except Exception as e:
        print(bcolors.FAIL + str(e) + bcolors.ENDC)
        if finish_upload:
            abort_multipart_upload(vault_name, upload_id)
