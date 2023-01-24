#! /bin/bash
tmux kill-session -t brazil
tmux new -d -s brazil
tmux send -t brazil "conda activate cp" ENTER
tmux send -t brazil "python3 collector_br.py" ENTER