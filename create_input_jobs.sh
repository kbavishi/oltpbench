#! /usr/bin/env bash

echo "Sanitizing input jobs trace file"
grep JOB ~/input_jobs.txt > /tmp/jobs.txt
cp /tmp/jobs.txt ~/input_jobs.txt
cut -f1 -d " " --complement ~/input_jobs.txt > /tmp/jobs.txt
sed -i 's/(GetTweet.java:35) INFO  - JOB GetTweet: /1,/' /tmp/jobs.txt
sed -i 's/(GetTweetsFromFollowing.java:45) INFO  - JOB GetTweetsFromFollowing: /2,/' /tmp/jobs.txt
sed -i 's/(GetFollowers.java:42) INFO  - JOB GetFollowers: /3,/' /tmp/jobs.txt
sed -i 's/(GetUserTweets.java:36) INFO  - JOB GetUserTweets: /4,/' /tmp/jobs.txt
sed -i 's/(InsertTweet.java:37) INFO  - JOB InsertTweet: /5,/' /tmp/jobs.txt
mv /tmp/jobs.txt ~/input_jobs.txt

echo "Fetching input job costs from PostgresSQL database"
python get_input_jobs_costs.py $1 $2 $3 false > ~/input_jobs_cost.txt

echo "Fetching input job locality-based costs from PostgresSQL database"
python get_input_jobs_costs.py $1 $2 $3 true > ~/input_jobs_loc_cost.txt

