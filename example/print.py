import sys
import numpy as np

print(np.fromstring(open(sys.argv[1], 'rb').read(), dtype=np.uint64))