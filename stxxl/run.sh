#!/bin/bash

if [ -e "/sys/fs/cgroup/memory/extmem" ]; then
    time sudo cgexec -g memory:extmem sudo -u ignat STXXLCFG=stxxl.conf build/main "$1"
else
    STXXLCFG=stxxl.conf build/main "$1"
fi

