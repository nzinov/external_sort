#%%
import numpy as np
import matplotlib.pyplot as plt

# %%
data = np.loadtxt('/home/nzinov/external_sort/example/res2.txt')
counts = data[::2, :]
data = data[1::2, :]

# %%
counts

# %%
plt.plot(np.log(data[:, 0]) / np.log(2), data[:, 1], label='wall time')
plt.plot(np.log(data[:, 0]) / np.log(2),  2e-3 * n / block_size * (np.log(n / sizes * 2) / np.log(sizes / block_size) + 1), label='theoretical limit')
plt.plot(np.log(data[:, 0]) / np.log(2), counts[:, 1] * 2e-3, label='block io num * 2e-3 s')
plt.plot(np.log(data[:, 0]) / np.log(2), data[:, 1] - cpu_time, label='io time')
plt.legend()
plt.xlabel('log data size')
plt.ylabel('time (s)')
plt.title('sorting 2GB')

# %%
block_size = 256 * 1024
sizes = data[:, 0]
n = 2 * 1024**3

# %%
n / block_size

# %%
cpu_time = np.array([
48.67+3.70,
44.39+3.43,
41.66+ 3.31,
40.97+ 3.61,
38.90+ 3.49,
38.26+ 3.89,
36.12+ 4.07,
])

# %%
data = np.loadtxt('/home/nzinov/external_sort/example/res2.txt')
counts = data[::2, :]
data = data[1::2, :]

# %%
counts

# %%
plt.plot(np.log(data[:, 0]) / np.log(2), data[:, 1], label='wall time')
plt.plot(np.log(data[:, 0]) / np.log(2),  2e-3 * n / block_size * np.log(n / sizes * 2) / np.log(sizes / block_size), label='theoretical limit')
plt.plot(np.log(data[:, 0]) / np.log(2), counts[:, 1] * 2e-3, label='block io num * 2e-3 s')
plt.plot(np.log(data[:, 0]) / np.log(2), data[:, 1] - cpu_time, label='io time')
plt.legend()
plt.xlabel('log data size')
plt.ylabel('time (s)')
plt.title('sorting 2GB')