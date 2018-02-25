#! /usr/bin/env bash

python dynamic_history_benchmark.py --rate=5 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1

python dynamic_history_benchmark.py --rate=10 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1

python dynamic_history_benchmark.py --rate=15 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1

python dynamic_history_benchmark.py --rate=20 --alpha=0.5 --gedf_factor=0.4 --iter=3 --fixed_deadline=false --random_page_cost=4.0 127.0.0.1
