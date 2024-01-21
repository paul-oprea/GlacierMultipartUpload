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

import argparse
import os
import queue
import sys
import threading

# The common variable
import time
import random

from GlacierChecksum import compute_bytearray_tree_hash, to_hex, __MEGABYTE__, compute_file_tree_hash
from GlacierMPU import initialize_context, upload_segment, abort_multipart_upload, finish_multipart_upload, bcolors

common_counter = 0
__CAPACITY__ = 3
# Lock for synchronizing access to the common variable
lock = threading.Lock()
q = queue.Queue(maxsize=__CAPACITY__)


def upload_segment_simulate(block, start, end, checksum, vault, uploadId):
    interval = random.randint(1, 30)
    print(f'Simulating upload of block starting at {start} waiting for {interval} seconds')
    time.sleep( interval )
    if interval % 3 == 0:
        raise Exception("Random exception")
    else:
        print(bcolors.OKBLUE + f"Upload of block starting at {start} ending at {end} DONE" + bcolors.ENDC)
def upload_thread():
    global common_counter
    while common_counter <= thread_max:
        try:
            with lock:
                common_counter += 1
            thread_id, block_contents, upload_id, start, end, tree_hash, vault_name = q.get()
            print(f"Thread {thread_id} got start segment {start} from queue of size {q.qsize()}")
            upload_segment(block_contents, start, end, to_hex(tree_hash), vault_name, upload_id)
        except Exception as e:
            print(bcolors.FAIL + f'Error in thread {thread_id} uploading segment {start} : {e}. Putting it back in queue'   + bcolors.ENDC)
            q.put((thread_id, block_contents, upload_id, start, end, tree_hash, vault_name))
            threading.Thread(target=upload_thread, daemon=True).start()
        finally:
            with lock:
                common_counter -= 1
            q.task_done()


# Open a binary file and return a handle
def open_binary_file(file_path):
    return open(file_path, 'rb')

def body_upload(f, file_size, vault_name, upload_id, blocksize, start_block = 0, end_block = -1 ):
    if end_block == -1:
        end_block = int(file_size / blocksize)

    # Creating multiple threads
    i = 0
    while start_block * blocksize <= min(file_size, end_block * blocksize):

        print('\nReading segment\t' + str(start_block + 1) + '\tout of \t' + str(int(file_size / blocksize) + 1))
        start = int(start_block * blocksize)
        f.seek(start)
        block = f.read(blocksize)
        end = int(start + len(block) - 1)

        start_block += 1
        tree_hash = compute_bytearray_tree_hash(block)
        print( f"\nHash value for start segment {start}:\t{to_hex(tree_hash)}")

        q.put([i, block, upload_id, start, end, tree_hash, vault_name])
        # threading.Thread(target=upload_thread, args=(i, block, upload_id, start, end, tree_hash, vault_name)).start()
        threading.Thread(target=upload_thread, daemon=True).start()

        i += 1
    # Waiting for all threads to complete
    q.join()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Glacier Multipart Uploader - Multithreaded")
    file_path, vault_name, upload_id, blocksize, start_block, end_block, finish_upload, thread_max = initialize_context(parser)
    if thread_max < 1:
        thread_max = __CAPACITY__
    q = queue.Queue(maxsize=thread_max-1)

    file_size = os.stat(file_path).st_size
    file = open_binary_file(file_path)
    try:
        body_upload(file, file_size, vault_name, upload_id, 128 * __MEGABYTE__, start_block, end_block)
        if finish_upload:
            response = finish_multipart_upload(vault_name, upload_id, file_size, compute_file_tree_hash(file_path))
            try:
                print('Completion attempt response HTTPStatusCode:\t' + str(response['ResponseMetadata']['HTTPStatusCode']))
                print(bcolors.OKGREEN + f'Successfully completed upload ID:\t{upload_id}' + bcolors.ENDC)
            except:
                print(bcolors.FAIL + 'Could not parse the response metadata' + bcolors.ENDC)
        print(f"Final value of common counter: {common_counter}")
    except:
        print(bcolors.FAIL + f"Unexpected error:{sys.exc_info()[0]}" + bcolors.ENDC)
        if finish_upload:
            abort_multipart_upload(vault_name, upload_id)
        file.close()
