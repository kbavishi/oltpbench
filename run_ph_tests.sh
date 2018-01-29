#! /usr/bin/env bash

# Arrival rate 50
python pred_history_twitter_benchmark.py --rate=50 --alpha=0.5 --gedf_factor=0.4 127.0.0.1
python pred_history_twitter_benchmark.py --rate=50 --alpha=0.5 --gedf_factor=0.2 127.0.0.1
python pred_history_twitter_benchmark.py --rate=50 --alpha=0.7 --gedf_factor=0.4 127.0.0.1
python pred_history_twitter_benchmark.py --rate=50 --alpha=0.7 --gedf_factor=0.2 127.0.0.1
python pred_history_twitter_benchmark.py --rate=50 --alpha=0.3 --gedf_factor=0.4 127.0.0.1
python pred_history_twitter_benchmark.py --rate=50 --alpha=0.3 --gedf_factor=0.2 127.0.0.1

# Arrival rate 75
python pred_history_twitter_benchmark.py --rate=75 --alpha=0.5 --gedf_factor=0.4 127.0.0.1
python pred_history_twitter_benchmark.py --rate=75 --alpha=0.5 --gedf_factor=0.2 127.0.0.1
python pred_history_twitter_benchmark.py --rate=75 --alpha=0.7 --gedf_factor=0.4 127.0.0.1
python pred_history_twitter_benchmark.py --rate=75 --alpha=0.7 --gedf_factor=0.2 127.0.0.1
python pred_history_twitter_benchmark.py --rate=75 --alpha=0.3 --gedf_factor=0.4 127.0.0.1
python pred_history_twitter_benchmark.py --rate=75 --alpha=0.3 --gedf_factor=0.2 127.0.0.1
