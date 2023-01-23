#! /bin/bash
tmux kill-session -t colombia
tmux new-session -d -s colombia
tmux send -t colombia "conda activate co" ENTER
tmux send -t colombia "python3 collector_co.py" ENTER