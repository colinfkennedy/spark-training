#!/bin/sh
#------------------------------------------------------------------------------
# Feeds data to the streaming application.
# Uses a hack. The streaming textFileStream method only reads files that are
# new. So, this script copies one new file from a data directory to a "tmp"
# directory, splitting it into 10000-line files. One of those is moved to the
# tmp directory every 5 seconds. This splitting and metered writing is a crude
# form of throttling.
#------------------------------------------------------------------------------

aadata="$PWD/data/airline-flights/alaska-airlines"
alldata="$PWD/data/airline-flights/csv"
: ${DATA:=$aadata}

tmp="$PWD/data/airline-flights/tmp"
tmp1="$PWD/data/airline-flights/tmp1"

case "$1" in
  -h|--h*)
    echo "usage: $0"
    exit 0
    ;;
esac

clean() {
  rm -rf $tmp $tmp1
}

function handle_signal {
  let status=$?
  trap "" SIGHUP SIGINT
  clean
  echo "Exiting..."
  exit $status
}

trap "handle_signal" SIGHUP SIGINT

mktmp() {
  local t=$1
  rm -rf $t
  mkdir -p $t
  if [ $? -ne 0 ]
  then
    echo "Could not create temporary directory $t"
    exit 1
  fi
}
mktmp $tmp
mktmp $tmp1
cd $tmp1

for i in {1..100}
do
  for year in {2000..2008}
  do
    file="$DATA/$year.csv"
    if [ -e $file ]         # Only data for 2008 is distributed w/ the repo.
    then
      echo "Processing file $file" 1>&2
      cat $file | split -l 100000
      for f in x*
      do
        echo "mv $f $tmp/$year-$f-$i.csv"
        mv $f $tmp/$year-$f-$i.csv
        sleep 5
      done
    fi
  done
done

clean
