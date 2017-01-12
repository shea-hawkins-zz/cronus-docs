#!/bin/sh

PYTHON=$(which python)

BUCKET="us-equity-taq"
MOUNTPOINT="/archives"

myprocess () {
 	$PYTHON s3client.py --bucket $BUCKET --mount-point $MOUNTPOINT
}

NOW=$(date +"%b-%d-%y")

mkdir -p "$MOUNTPOINT"

until myprocess; do
     echo "$NOW App has crashed. Restarting..."
     sleep 1
     sudo umount -f "$MOUNTPOINT"
done
