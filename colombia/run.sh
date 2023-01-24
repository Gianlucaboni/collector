#! /bin/bash
tmux kill-session -t colombia
tmux new -d -s colombia
tmux send -t colombia "conda activate cp" ENTER
tmux send -t colombia "python3 collector_co.py" ENTER