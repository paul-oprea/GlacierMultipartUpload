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

import hashlib
import sys
import os

__MEGABYTE__ = 1024 * 1024

def to_hex(data):
    return ''.join(f'{byte:02x}' for byte in data).lower()

# SHA256 hash of a bytearray
def block_hash(block):
    hash_object = hashlib.sha256()
    hash_object.update(block)
    return hash_object.digest()

# SHA256 hash of a pair of bytearrays
def pair_hash(left_block, right_block):
    hash_object = hashlib.sha256()
    hash_object.update(left_block + right_block)

    return hash_object.digest()

# Calculate the block hash of each element in an array
def calculate_hashes(list):
    hashes = []
    for block in list:
        hashes.append(block_hash(block))
    return hashes

# Calculate the tree hash of an array of bytearrays
def compute_tree_hash(hasheslist):
    previous_tree_layer = hasheslist
    while len(previous_tree_layer) > 1:
        left_boundary = len(previous_tree_layer) // 2
        if len(previous_tree_layer) % 2 == 1:
            left_boundary += 1

        current_level_layer = []
        current_level_counter = 0
        for i in range(left_boundary):
            if ( len(previous_tree_layer) - i * 2) > 1:
                current_level_layer.append(pair_hash(previous_tree_layer[i*2], previous_tree_layer[i*2 + 1]))
            else:
                current_level_layer.append(previous_tree_layer[i*2])

            current_level_counter += 1

        previous_tree_layer = current_level_layer

    return previous_tree_layer[0]

# Calculate the tree hash of a file in one megabyte chunks
def compute_file_tree_hash(file_name):
    filesize = os.stat(file_name).st_size
    counter = 0
    list = []
    with open(file_name, 'rb') as f:
        while counter * __MEGABYTE__ <= filesize:
            print('\rComputing treehash for the entire file %d%% ' % (int(counter * 100 * __MEGABYTE__ /filesize ) ), end='')
            start = int(counter * __MEGABYTE__)
            f.seek(start)
            block = f.read(__MEGABYTE__)
            end = int(start + len(block) - 1)
            list.append(block_hash(block))
            counter += 1

        # print( '\rThe AWS botocore would compute:\t' + botocore.utils.calculate_tree_hash(f))
    # calculate the tree hash of the hashes
    tree_hash = compute_tree_hash(list)
    return to_hex(tree_hash)

# Calculate the tree hash of a bytearray in one megabyte chunks
def compute_bytearray_tree_hash( block ):
    if len(block) > __MEGABYTE__:
        # iterate through the block by megabyte
        i = 0
        blocklist = []
        while (i < len(block)):
            tree_hash = block_hash(block[i:i + __MEGABYTE__])
            blocklist.append(tree_hash)
            i += __MEGABYTE__
        tree_hash = compute_tree_hash(blocklist)
    else:
        tree_hash = block_hash(block)  # for a single block, the tree hash is the same as the block hash
    return tree_hash


if __name__ == '__main__':
    file_name = sys.argv[1]
    print(compute_file_tree_hash(file_name))


