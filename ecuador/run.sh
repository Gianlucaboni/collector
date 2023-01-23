#! /bin/bash
tmux kill-session -t ecuador
tmux new-session -d -s ecuador
tmux send -t ecuador "conda activate cp" ENTER
tmux send -t ecuador "python3 collector_ec.py" ENTER