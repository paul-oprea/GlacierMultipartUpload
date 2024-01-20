import argparse
import os
import sys
import threading

# The common variable
import time
import random

from GlacierChecksum import compute_bytearray_tree_hash, to_hex, __MEGABYTE__, compute_file_tree_hash
from GlacierMPU import initialize_context, upload_segment, abort_multipart_upload, finish_multipart_upload

common_counter = 0
__CAPACITY__ = 3
# Lock for synchronizing access to the common variable
lock = threading.Lock()

def upload_thread(thread_id, block_contents, upload_id, start, end, checksum, vault_name):
    global common_counter
    tree_hash = compute_bytearray_tree_hash(block_contents)
    print( "\nThread\t" + str(thread_id) + "\tcomputing hash value \t" + to_hex(tree_hash))
    with lock:
        common_counter += 1
    try:
        upload_segment(block_contents, start, end, to_hex(tree_hash), vault_name, upload_id)
    except Exception as e:
        print(f"Error in thread {thread_id} uploading segment {start} : {e}")
    finally:
        with lock:
            common_counter -= 1


# Open a binary file and return a handle
def open_binary_file(file_path):
    return open(file_path, 'rb')

def body_upload(f, file_size, vault_name, upload_id, blocksize, start_block = 0, end_block = -1 ):
    if end_block == -1:
        end_block = int(file_size / blocksize)

    # Creating multiple threads
    threads = []
    i = 0
    while start_block * blocksize <= min(file_size, end_block * blocksize):

        print('\nReading segment\t' + str(start_block + 1) + '\tout of \t' + str(int(file_size / blocksize) + 1))
        start = int(start_block * blocksize)
        f.seek(start)
        block = f.read(blocksize)
        end = int(start + len(block) - 1)

        start_block += 1
        while common_counter > thread_max:
            print(f"Reached parallel quota, waiting for active threads to finish...")
            time.sleep(1)
        thread = threading.Thread(target=upload_thread, args=(i, block, upload_id, start, end, '', vault_name))
        threads.append(thread)
        thread.start()

        i += 1
    # Waiting for all threads to complete
    for thread in threads:
        thread.join()



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Glacier Multipart Uploader - Multithreaded")
    file_path, vault_name, upload_id, blocksize, start_block, end_block, finish_upload, thread_max = initialize_context(parser)
    if thread_max < 1:
        thread_max = __CAPACITY__

    file_size = os.stat(file_path).st_size
    file = open_binary_file(file_path)
    try:
        body_upload(file, file_size, vault_name, upload_id, 128 * __MEGABYTE__, start_block, end_block)
        if finish_upload:
            response = finish_multipart_upload(vault_name, upload_id, file_size, compute_file_tree_hash(file_path))
        try:
            print('Completion attempt response HTTPStatusCode:\t' + str(response['ResponseMetadata']['HTTPStatusCode']))
            print('Successfully completed upload ID:\t' + upload_id)
        except:
            print('Could not parse the response metadata')
        print(f"Final value of common counter: {common_counter}")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        if finish_upload:
            abort_multipart_upload(vault_name, upload_id)
        file.close()
