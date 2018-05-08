#! /usr/bin/env bash

echo "Sanitizing input jobs trace file"
grep JOB input_jobs_trace.txt > /tmp/jobs.txt
cp /tmp/jobs.txt input_jobs.txt
cut -f1 -d " " --complement input_jobs.txt > /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetTweet: /1,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetTweetsFromFollowing: /2,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetFollowers: /3,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetUserTweets: /4,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB InsertTweet: /5,/' /tmp/jobs.txt
mv /tmp/jobs.txt input_jobs.txt

echo "Sanitizing random jobs trace file"
grep JOB random_jobs_trace.txt > /tmp/jobs.txt
cp /tmp/jobs.txt random_jobs.txt
cut -f1 -d " " --complement random_jobs.txt > /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetTweet: /1,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetTweetsFromFollowing: /2,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetFollowers: /3,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB GetUserTweets: /4,/' /tmp/jobs.txt
sed -i 's/(.*.java:.*) INFO  - JOB InsertTweet: /5,/' /tmp/jobs.txt
mv /tmp/jobs.txt random_jobs.txt

FAIL=0
echo "Sanitizing other input jobs trace files"
for ((i=0; i<5; i++)); do
    grep JOB "random_jobs_trace$i.txt" > /tmp/jobs.txt
    cp /tmp/jobs.txt random_jobs$i.txt
    cut -f1 -d " " --complement random_jobs$i.txt > /tmp/jobs.txt
    sed -i 's/(.*.java:.*) INFO  - JOB GetTweet: /1,/' /tmp/jobs.txt
    sed -i 's/(.*.java:.*) INFO  - JOB GetTweetsFromFollowing: /2,/' /tmp/jobs.txt
    sed -i 's/(.*.java:.*) INFO  - JOB GetFollowers: /3,/' /tmp/jobs.txt
    sed -i 's/(.*.java:.*) INFO  - JOB GetUserTweets: /4,/' /tmp/jobs.txt
    sed -i 's/(.*.java:.*) INFO  - JOB InsertTweet: /5,/' /tmp/jobs.txt
    mv /tmp/jobs.txt random_jobs$i.txt

    echo "Fetching Markov model job costs for random_jobs${i}"
    python final_input_job_costs.py $1 $2 $3 random_jobs$i.txt random_jobs_loc_cost$i.txt &

    echo "Fetching Postgres job costs for random_jobs${i}"
    python get_input_jobs_costs.py $1 $2 $3 random_jobs$i.txt random_jobs_cost$i.txt &
done

for job in `jobs -p`
do
    echo $job
    wait $job || let "FAIL+=1"
done

if [ "$FAIL" == "0" ];
then
    echo "All random input trace jobs passed!"
else
    echo "Random input trace jobs failed! Failure count: ($FAIL)"
fi

echo "Fetching new input job costs from PostgresSQL database"
#python new_input_jobs_costs.py $1 $2 $3 true > input_jobs_loc_cost_new.txt
#python final_input_job_costs.py $1 $2 $3 &

echo "Fetching input job costs from PostgresSQL database"
#python get_input_jobs_costs.py $1 $2 $3 &
