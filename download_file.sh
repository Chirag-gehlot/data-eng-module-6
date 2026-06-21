#!/bin/bash

set -e

TAXI_TYPE=$1

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for YEAR in 2020 2021; do
  for MONTH in {1..12}; do
    FMONTH=$(printf "%02d" ${MONTH})

    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "Downloading ${URL} to ${LOCAL_PATH}"
    mkdir -p "${LOCAL_PREFIX}"

    wget "${URL}" -O "${LOCAL_PATH}"

    echo "Extracting ${LOCAL_PATH}"
    gunzip -f "${LOCAL_PATH}"

  done
done