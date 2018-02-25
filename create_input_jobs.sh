#! /usr/bin/env bash

echo "Sanitizing input jobs trace file"
grep JOB input_jobs_trace.txt > /tmp/jobs.txt
cp /tmp/jobs.txt ~/input_jobs.txt
cut -f1 -d " " --complement ~/input_jobs.txt > /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetTweet: /1,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetTweetsFromFollowing: /2,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetFollowers: /3,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetUserTweets: /4,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB InsertTweet: /5,/' /tmp/jobs.txt
mv /tmp/jobs.txt ~/input_jobs.txt

echo "Fetching new input job costs from PostgresSQL database"
python new_input_jobs_costs.py $1 $2 $3 true > ~/input_jobs_loc_cost_new.txt

echo "Fetching input job costs from PostgresSQL database"
python get_input_jobs_costs.py $1 $2 $3
