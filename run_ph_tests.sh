#! /usr/bin/env bash

python pred_history_twitter_benchmark.py --rate=75 --alpha=0.7 --gedf_factor=0.4 --iter=5 --fixed_deadline=false --random_page_cost=60.6847 127.0.0.1

python pred_history_twitter_benchmark.py --rate=100 --alpha=0.7 --gedf_factor=0.4 --iter=5 --fixed_deadline=false --random_page_cost=60.6847 127.0.0.1

python pred_history_twitter_benchmark.py --rate=125 --alpha=0.7 --gedf_factor=0.4 --iter=5 --fixed_deadline=false --random_page_cost=60.6847 127.0.0.1
