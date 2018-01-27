#! /usr/bin/env bash

sudo service postgresql stop
echo 3 | sudo tee /proc/sys/vm/drop_caches
sudo service postgresql start
