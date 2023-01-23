#! /bin/bash
tmux kill-session -t bolivia
tmux new-session -d -s bolivia
tmux send -t bolivia "conda activate cp" ENTER
tmux send -t bolivia "python3 collector_bo.py" ENTER