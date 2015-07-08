#!/bin/sh
#------------------------------------------------------------------------------
# Feeds data through a server socket managed by "nc".
# Performs crude buffering; it sends $BLOCKSIZE lines, then sleeps for 1 second.
# It reads all the CSV files in the $DATA directory, which defaults to
# "data/airline-flights/alaska-airlines", which is included with the
# project data set (flights for Alaska Air in 2008). If you download
# additional CSV files from the source described in "data/README", put
# them in another directory and point $DATA there. Note the "alldata"
# variable below. try the "-h" option.
#------------------------------------------------------------------------------

aadata=data/airline-flights/alaska-airlines
alldata=data/airline-flights/csv
: ${DATA:=$aadata}
: ${BLOCKSIZE:=10000}      # Number of lines to send uninterrupted.
: ${PORT:=8800}

function handle_signal {
  let status=$?
  trap "" SIGHUP SIGINT
  echo "Exiting..."
  exit $status
}

trap "handle_signal" SIGHUP SIGINT

help() {
      cat <<EOF
usage: $0 [-h | --help] [-b | --block N] [-d | --dir directory] [-p | --port P]
where:
  -b N    Use N as the number of lines in a block between paused (default: $BLOCKSIZE)
  -d dir  Use "dir" as the directory for input (default: $DATA)
  -p P    Use port "P" (default: $PORT)
EOF
}

while [ $# -gt 0 ]
do
  case $1 in
    -h|--h*)
      help
      exit 0
      ;;
    -b|--b*)
      shift
      BLOCKSIZE=$1
      ;;
    -d|--d*)
      shift
      DATA=$1
      ;;
    -p|--p*)
      shift
      PORT=$1
      ;;
    *)
      echo "Unrecognized option: $1"
      help
      exit 1
      ;;
  esac
  shift
done

feed() {
  for year in {2000..2008}  # Or start at an earlier year; 1987 is the earliest.
  do
    file="$DATA/$year.csv"
    if [ -e $file ]
    then
      printf "processing: $file " 1>&2
      let count=0
      while read line
      do
        echo $line
        let count=$count+1
        if [ $count -eq $BLOCKSIZE ]
        then
            let count=0
            printf "." 1>&2
            sleep 1
        fi
      done < $file
    fi
  done
}

echo "Feeding data to port $PORT"
feed | /usr/bin/nc -l $PORT
