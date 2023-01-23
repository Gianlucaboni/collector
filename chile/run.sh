#! /bin/bash
tmux kill-session -t chile
tmux new-session -d -s chile
tmux send -t chile "conda activate cp" ENTER
tmux send -t chile "python3 collector_ch.py" ENTER