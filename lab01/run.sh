#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Использование: $0 WAP12.txt"
    exit 1
fi

for file in "$@"; do
    python3 main.py "$file"
done