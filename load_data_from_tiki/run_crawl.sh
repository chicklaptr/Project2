#!/bin/bash

while true; do
    python3 problem.py
    EXIT_CODE=$?

    if [ $EXIT_CODE -ne 0 ]; then
        echo "Python crashed or failed, retrying in 5 seconds..."
        sleep 5
    else
        echo "Python finished successfully!"
        break
    fi
done

