#!/bin/bash
OUT=runTestResults.txt
FULL=$OUT.full
LINEAR=
EXCLUSIVITY=
function runTest {
	# Get the params
	BIN=$1
	DB=$2
	NOTE=$3

	# Capture the results in a temp file
	rm -f $OUT
	echo `date` " Starting: $NOTE ($BIN, $DB)" >> $OUT

	# Run the test
	$BIN -csv -numa -numastats -mmap $LINEAR $EXCLUSIVITY -testSeconds 60 -maxNumThreads 256 -dbFilename $DB >> $OUT

	# Share the results
	echo `date` " Done" >> $OUT
	cat $OUT | mail -s "runTest: $NOTE ($BIN, $DB)" dbarrett@expensify.com
	cat $OUT >> $FULL
}

# Reset the caches
#echo `date` " Resetting cache and precaching..." >> $FULL
#free -h >> $FULL
#sh -c "echo 3 > /proc/sys/vm/drop_caches"
#md5sum /var/lib/bedrock/perftest.30B.db > /dev/null
#free -h >> $FULL
#echo | mail -s "runTest: Prefetched ($DB)" dbarrett@expensify.com


# Test vs non-exclusive
LINEAR=-linear
#EXCLUSIVITY='-vms unix-excl -locking_mode NORMAL'
#runTest ./perftest_customLockVFS /var/lib/bedrock/perftest.30B.db "$EXCLUSIVITY"
#EXCLUSIVITY='-vms unix-none -locking_mode EXCLUSIVE'
#runTest ./perftest_customLockVFS /var/lib/bedrock/perftest.30B.db "$EXCLUSIVITY"
#EXCLUSIVITY='-vms unix -locking_mode NORMAL'
#runTest ./perftest_customLockVFS /var/lib/bedrock/perftest.30B.db "$EXCLUSIVITY"
EXCLUSIVITY='-vms unix-excl -locking_mode NORMAL'
runTest ./perftest_lockfix /var/lib/bedrock/perftest.30B.db "$EXCLUSIVITY"
