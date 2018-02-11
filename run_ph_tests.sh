#! /usr/bin/env bash

python pred_history_twitter_benchmark.py --rate=50 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=true --random_page_cost=4.0 127.0.0.1

python pred_history_twitter_benchmark.py --rate=50 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=true --random_page_cost=4.0 127.0.0.1

python pred_history_twitter_benchmark.py --rate=100 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1

python pred_history_twitter_benchmark.py --rate=100 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=true --random_page_cost=4.0 127.0.0.1

python pred_history_twitter_benchmark.py --rate=150 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1

python pred_history_twitter_benchmark.py --rate=150 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=true --random_page_cost=4.0 127.0.0.1

python pred_history_twitter_benchmark.py --rate=200 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1

python pred_history_twitter_benchmark.py --rate=200 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=true --random_page_cost=4.0 127.0.0.1
