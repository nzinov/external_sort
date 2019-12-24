import sys
import numpy as np

size = int(sys.argv[1])
data = np.arange(size, dtype=np.uint64)
with open('sample.gt', 'wb') as f:
    f.write(data.tostring())
np.random.shuffle(data)
with open('sample.in', 'wb') as f:
    f.write(data.tostring())
