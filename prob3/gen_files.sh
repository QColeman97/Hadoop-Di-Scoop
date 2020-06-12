#!/bin/sh

ORIG_LEN=158980
# Loop makes shorter files
for i in {25000..150000..25000}; do
	val=$((i+1))
	sed ''"$val"',$d' us-covid-counties.csv > ucc-$i.csv
done

# Loops make longer files each
top=$(((300000 - $ORIG_LEN) / 3))
for ((i=1;i<=top;i++)); do
	echo "$2021-05-19,Los Angeles,California,06037,39573,1913" >> ucc-300000.csv
	echo "$2021-05-19,San Luis Obispo,California,06037,39573,1913" >> ucc-300000.csv
	echo "$2021-05-19,San Francisco,California,06037,39573,1913" >> ucc-300000.csv
done
top=$(((500000 - $ORIG_LEN) / 3))
for ((i=1;i<=top;i++)); do
	echo "$2021-05-19,Los Angeles,California,06037,39573,1913" >> ucc-500000.csv
	echo "$2021-05-19,San Luis Obispo,California,06037,39573,1913" >> ucc-500000.csv
	echo "$2021-05-19,San Francisco,California,06037,39573,1913" >> ucc-500000.csv
done
top=$(((1000000 - $ORIG_LEN) / 3))
for ((i=1;i<=top;i++)); do
	echo "2021-05-19,Los Angeles,California,06037,39573,1913" >> ucc-1000000.csv
	echo "2021-05-19,San Luis Obispo,California,06037,39573,1913" >> ucc-1000000.csv
	echo "2021-05-19,San Francisco,California,06037,39573,1913" >> ucc-1000000.csv
done
top=$(((4000000 - $ORIG_LEN) / 3))
for ((i=1;i<=top;i++)); do
	echo "2021-05-19,Los Angeles,California,06037,39573,1913" >> ucc-4000000.csv
	echo "2021-05-19,San Luis Obispo,California,06037,39573,1913" >> ucc-4000000.csv
	echo "2021-05-19,San Francisco,California,06037,39573,1913" >> ucc-4000000.csv
done
top=$(((16000000 - $ORIG_LEN) / 3))
for ((i=1;i<=top;i++)); do
	echo "2021-05-19,Los Angeles,California,06037,39573,1913" >> ucc-16000000.csv
	echo "2021-05-19,San Luis Obispo,California,06037,39573,1913" >> ucc-16000000.csv
	echo "2021-05-19,San Francisco,California,06037,39573,1913" >> ucc-16000000.csv
done

