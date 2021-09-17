readonly TAG=$1
readonly DOCKER_IMAGE=nachocode/experiments-suite
sbt assembly && docker build -t "$DOCKER_IMAGE:$TAG" .
