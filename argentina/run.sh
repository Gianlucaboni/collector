#! /bin/bash

tmux kill-session -t argentina
tmux new-session -d -s argentina
tmux send -t argentina "source activate cp" ENTER
tmux send -t argentina "python3 collector_ar.py" ENTER