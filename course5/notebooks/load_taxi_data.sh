set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020
URL_PREFIX=https://s3.amazonaws.com/nyc-tlc/trip+data

for MONTH in $(seq 1 12); do
        FMONTH=`printf "%02d" ${MONTH}`
      	URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"

	LOCAL_PATH="data/raw/${TAXI_TYPE}/${YEAR}"
	LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${MONTH}.csv"
 
	mkdir -p ${LOCAL_PATH}

	echo downloading ${URL}
	wget ${URL} -O ${LOCAL_PATH}/${LOCAL_FILE};
        #touch ${LOCAL_PATH}/${LOCAL_FILE};

	gzip ${LOCAL_PATH}/${LOCAL_FILE}
done

