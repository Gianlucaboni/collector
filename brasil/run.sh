#! /bin/bash
tmux kill-session -t brazil
tmux new -d -s brazil
tmux kill-session -t brazil2
tmux new -d -s brazil2
tmux send -t brazil "conda activate cp" ENTER
tmux send -t brazil "python3 collector_br.py" ENTER
tmux send -t brazil2 "conda activate cp" ENTER
tmux send -t brazil2 "python3 collector_br2.py" ENTER