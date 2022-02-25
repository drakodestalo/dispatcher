# Parallel-jobs

This is a dispatcher script, it allows to run multiple instances os a given script and pass a date parameter which is included between a given time window

it uses a queue implementation as exampled in https://hackthology.com/a-job-queue-in-bash.html

###### Input parameters:

```
-s start date
-e end date
-m a manual file containing list of dates to be processed (not mandatory)
-j number of concurrent jobs
-p payload (script to be executed), this parameter can be re-inserted as many times as needed, it will be created one execution per date per payload
-h Specify a home directory for the payload(s)
```

###### Example:

Regular run:
```
./dispatcher.sh -p payload.sh -s 20220101 -e 20220131 -j 2
```
If we need to process a manual list of dates:
./dispatcher.sh -p payload.sh -s 20220101 -e 20220131 -j 2 -m error.dates
