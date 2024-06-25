#!/bin/bash

function stop_instance() {
    cloud-init clean -s
    shutdown -h now
}

MOUNT_POINT="/var/lib/clickhouse/vector_index_cache"

devices=$(lsblk -ln -o NAME | grep -E '^nvme[0-9]n1$')

unmounted_device=""
for device in $devices; do
    mountpoint=$(lsblk -ln -o MOUNTPOINT "/dev/$device")
    if [ -z "$mountpoint" ]; then
        unmounted_device=$device
        break
    fi
done

if [ -z "$unmounted_device" ]; then
    echo "No unmount nvme*1 device found."
    stop_instance
fi

mkdir -p $MOUNT_POINT

mkfs.ext4 -F "/dev/$unmounted_device"

mount "/dev/$unmounted_device" $MOUNT_POINT

if mount | grep -q "$MOUNT_POINT"; then
    echo "Block /dev/$unmounted_device success mount."
else
    echo "Mount Failed!"
    stop_instance
fi

chown -R ec2-user:ec2-user $MOUNT_POINT

