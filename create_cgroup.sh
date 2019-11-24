#!/bin/bash

MEMORY=$(((16 + 1024) * 1024 * 1024))

cd /sys/fs/cgroup/memory/
mkdir extmem
cd extmem
echo $MEMORY > memory.limit_in_bytes
echo $MEMORY > memory.dirty_limit_in_bytes
echo $MEMORY > memory.anon.limit
