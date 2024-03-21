#! /bin/bash
tmux kill-session -t brazil
tmux new -d -s brazil
tmux kill-session -t brazil2
tmux new -d -s brazil2
tmux send -t brazil "conda activate cp" ENTER
tmux send -t brazil2 "conda activate cp" ENTER
tmux send -t brazil "python collector_br.py -csv ./cities.csv -o brasil1.txt" ENTER
tmux send -t brazil2 "python collector_br.py -csv ./cities2.csv -o brasil2.txt" ENTER