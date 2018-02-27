#! /usr/bin/env bash

python dynamic_history_benchmark.py --rate=50 --alpha=0.5 --gedf_factor=0.4 --iter=5 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1

python dynamic_history_benchmark.py --rate=100 --alpha=0.5 --gedf_factor=0.4 --iter=5 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1

python dynamic_history_benchmark.py --rate=150 --alpha=0.5 --gedf_factor=0.4 --iter=5 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1
