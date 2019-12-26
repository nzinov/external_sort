import sys
import os
import time
block_size = 256 * 1024
size = block_size * 1000
while size <= 2 * 1024**3:
    start = time.time()
    os.system("time ./main /mnt/storage/nzinov/20GB.in sample.out . {} {}".format(size, block_size))
    end = time.time()
    print(size, end - start)
    sys.stdout.flush()
    size *= 2
