#!/bin/sh

WORKSPACE_MOUNT=/workspace
WORKSPACE_FOLDER=/workspace/skiplistmap

if [ ! -d $WORKSPACE_FOLDER ]; then
    mkdir -p "$WORKSPACE_FOLDER"
fi
chown -R vscode:vscode "$WORKSPACE_MOUNT"

while sleep 1000; do :; done