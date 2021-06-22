#!/bin/bash

mkdir -pv /run/sshd && /usr/sbin/sshd -D $@
