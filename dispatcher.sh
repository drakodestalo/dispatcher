#!/bin/bash

# TO-DO:

# For -m parameter, we need to validate that dates in the file has the expected format YYYYMMDD
# Validate -f value 
START=$(mktemp -t start-XXXX)
FIFO=$(mktemp -t fifo-XXXX)
FIFO_LOCK=$(mktemp -t lock-XXXX)
START_LOCK=$(mktemp -t lock-XXXX)
WORK_FILE=$(mktemp -t work-XXXX)
SUMMARY=$(mktemp -t summary-XXXX)


declare -a payload
payload_home=""
completed_jobs=0

# This is the actual job to run
job() {
  self_pid=$$
  i=$1
  work=$2
  if [ -z ${payload_home} ]; then 
    work_payload=$3
  else
    work_payload="${payload_home}/$3"
  fi 

  logfile=$(mktemp -t parallel-XXXX)
  echo "Starting job #$i, pid: $self_pid, script: $3, date $work, logfile $logfile"
  time eval ${work_payload} ${work} > $logfile 2>&1
  let "completed_jobs=completed_jobs+1"
  echo "Done with job #$i"
  grep "tracking URL" $logfile | uniq 
  status=$( grep "PYSPARK status:" $logfile | grep -v UNDEFINED )

  echo $status
  echo "job $i, script: $3, date $work, status $status" >> $SUMMARY

}

# make the files
## create a trap to cleanup on exit if we fail in the middle.
cleanup() {
  rm $FIFO $START $FIFO_LOCK $START_LOCK $WORK_FILE
}

## This is the worker to read from the queue.
work() {
  ID=$1
  ## first open the fifo and locks for reading.
  exec 3<$FIFO
  exec 4<$FIFO_LOCK
  exec 5<$START_LOCK

  ## signal the worker has started.
  flock 5                 # obtain the start lock
  echo $ID >> $START      # put my worker ID in the start file
  flock -u 5              # release the start lock
  exec 5<&-               # close the start lock file
  echo worker $ID started

  while true; do
    ## try to read the queue
    flock 4                      # obtain the fifo lock
    read -su 3 work_id work_item work_payload # read into work_id and work_item
    read_status=$?               # save the exit status of read
    flock -u 4                   # release the fifo lock

    ## check the line read.
    if [[ $read_status -eq 0 ]]; then
      ## If read gives an exit code of 0 the read succeeded.
      # got a work item. do the work
      # echo $ID got work_id=$work_id work_item=$work_item
      ## Run the job in a subshell. That way any exit calls do not kill
      ## the worker process.
      ( job "$work_id" "$work_item" "$work_payload" )
    else
      ## Any other exit code indicates an EOF.
      break
    fi
  done
  # clean up the fd(s)
  exec 3<&-
  exec 4<&-
  echo $ID "done working"
}

## utility function to send the jobs to the workers
send() {
  work_id=$1
  work_item=$2
  work_payload=$3
  echo sending $work_id $work_item
  echo "$work_id" "$work_item" "$work_payload" 1>&3 ## the fifo is fd 3
}


script_stats() {
  echo "Execution start date: $start_exec_date"
  echo "Completed jobs: $completed_jobs"
}

### Begin execution of the script
echo "Starting dispatcher, PID: $$"
trap cleanup SIGTERM
trap script_stats SIGUSR1

## mktemp makes a regular file. Delete that an make a fifo.
rm $FIFO
mkfifo $FIFO
echo $FIFO

date_format="%Y%m%d"

# Parse input values
while getopts ":s:e:j:p:h:m:" opt; do
  case $opt in
    s) start_date="$OPTARG"
    ;;
    e) end_date="$OPTARG"
    ;;
    m) manual_date="$OPTARG"
    ;;
    j) WORKERS="$OPTARG"
    ;;
    p) payload+=($OPTARG)
    ;;
    h) payload_home="$OPTARG"
    ;;
    f) date_format="${OPTARG}"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

# input validation
if [[ ( -z "${start_date}"  || -z "${end_date}" ) && -z "${manual_date}" ]] ; then
  echo "Please set the start/end date with -s and -e"
  exit 1
fi

if [ -z "${WORKERS}" ]; then
  WORKERS=1
fi

if [ -z "${payload}" ]; then
  echo "Please set a payload with -p"
  exit 1
fi
start_exec_time=$( date )
echo "Start date: ${start_date}"
echo "End date: ${end_date}"
echo "No. Workers: ${WORKERS}"
echo "Payload: ${payload}"
echo "Execution report: ${SUMMARY}"

## Start the workers.
for ((i=1;i<=$WORKERS;i++)); do
  work $i &
done

## Open the fifo for writing.
exec 3>$FIFO
## Open the start lock for reading
exec 4<$START_LOCK

## Wait for the workers to start
while true; do
  flock 4
  started=$(wc -l $START | cut -d \  -f 1)
  flock -u 4
  if [[ $started -eq $WORKERS ]]; then
    break
  else
    echo waiting, started $started of $WORKERS
  fi
done
exec 4<&-

## Produce the jobs to run.
i=0

if [ -z "${manual_date}" ]; then

    while [ $start_date -le $end_date ]; do
      echo "${start_date}" >>  ${WORK_FILE}
      start_date=$(date -d "${start_date} +1 day" +${date_format})
    done

else
    
    cat ${manual_date} > ${WORK_FILE}
    
fi

while read item; do
  for p in "${payload[@]}"; do
    send $i $item $p
    i=$((i+1))
  done
done < ${WORK_FILE}
## close the filo
exec 3<&-
## disable the cleanup trap
trap '' 0

cleanup
## now wait for all the workers.
wait
total_jobs=$( wc -l $SUMMARY )
failed_jobs=$( grep -i FAILED $SUMMARY | wc -l )

##output not processed dates
errors=$( grep -oP '\d{8}(?=[^d]+FAILED)' $SUMMARY ) 
[[ ! -z "$errors" ]]  && echo "$errors" > error_date.err_$$

succeeded_jobs=$( grep -i -E "SUCCEEDED|success" $SUMMARY | wc -l )
end_exec_time=$( date )
echo "Total jobs executed: $total_jobs, succeded: $succeeded_jobs, failed: $failed_jobs"
echo "Start time: $start_exec_time"
echo "End time: $end_exec_time"