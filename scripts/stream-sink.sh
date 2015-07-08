#!/bin/sh
#---------------------------------------
# Runs the Spark Streaming app that reads the data.
#---------------------------------------

help() {
      cat <<EOF
usage: $0 [-h | --help] [-s | -d] [--sql] [main_options]
where:
  -s           Use socket input at address localhost:8800  (default)
  -d           Use directory input (uses the default location in the app.)
  --sql        Run the SQL version of the Streaming app.
  main_options Other options supported by SparkStreaming or SparkStreamingSQL.
               These options must come LAST.
EOF
}

defaultArgs=("--socket" "localhost:8800")
args=(${defaultArgs[@]})
app=SparkStreaming
while [ $# -gt 0 ]
do
  case $1 in
    -h|--h*)
      help
      exit 0
      ;;
    -s)
      args=(${defaultArgs[@]})
      ;;
    -d)
      args=()
      ;;
    --sql)
      app=SparkStreamingSQL
      ;;
    "")
      ;;
    *)
      break
      ;;
  esac
  shift
done
args=(${args[@]} $@)

run() {
  # SBT has bugs where some arguments are dropped by the time they reach "run-main",
  # but redirecting input to it seems to work.
  echo "running: com.typesafe.training.sws.ex8.$app ${args[@]}"
  echo "run-main com.typesafe.training.sws.ex8.$app ${args[@]}" | sbt
}

# Remove very annoying warnings that are harmless, but occur for
# single-machine "clusters".
run 2>&1 | grep -v "WARN BlockManager: Block .* already exists"
# now=$(date "+%Y-%m-%d_%H-%M-%S")
# run 2>&1 | grep -v "WARN BlockManager: Block .* already exists" | tee output/spark-streaming-$now.log
