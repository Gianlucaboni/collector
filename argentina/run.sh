#! /bin/bash

tmux kill-session -t argentina
tmux new -d -s argentina
tmux send-keys -t argentina "source activate cp" C-m
tmux send-keys -t argentina "python3 collector_ar.py" C-m