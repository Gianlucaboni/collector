#! /bin/bash
tmux kill-session -t peru
tmux new -d -s peru
tmux send -t peru "conda activate cp" ENTER
tmux send -t peru "python3 collector_pe.py" ENTER