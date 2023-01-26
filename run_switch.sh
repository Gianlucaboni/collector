#! /bin/bash

tmux kill-session -t switch
tmux new -d -s switch
tmux send-keys -t switch "source activate cp" C-m
tmux send-keys -t switch "python3 switch.py" C-m