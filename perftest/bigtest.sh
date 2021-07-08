TEST_SECONDS=30
echo "==================================================================="
echo "   Starting new test: " `date`

echo "-------------------------------------------------------------------"
cat perftest.cpp

echo "-------------------------------------------------------------------"
cat bigtest.sh

echo "-------------------------------------------------------------------"
echo "Building db..."
time sqlite3 /var/lib/bedrock/perftest.30B.db < perftest.sql

echo "-------------------------------------------------------------------"
echo "cpupower -c all set -b 0"
cpupower -c all set -b 0

echo "-------------------------------------------------------------------"
free -h
sh -c "echo 3 > /proc/sys/vm/drop_caches"
free -h
md5sum /var/lib/bedrock/perftest.30B.db > /dev/null
./perftest.make_SQLITE_SHARED_MAPPING -csv -numa -numastats -mmap -testSeconds 120 -maxNumThreads 256 -dbFilename /var/lib/bedrock/perftest.30B.db

echo "-------------------------------------------------------------------"
free -h
sh -c "echo 3 > /proc/sys/vm/drop_caches"
free -h
md5sum /var/lib/bedrock/perftest.30B.db > /dev/null
./perftest.make -csv -numa -numastats -mmap -testSeconds 120 -maxNumThreads 256 -dbFilename /var/lib/bedrock/perftest.30B.db

echo "-------------------------------------------------------------------"
free -h
sh -c "echo 3 > /proc/sys/vm/drop_caches"
free -h
md5sum /var/lib/bedrock/perftest.30B.db > /dev/null
./perftest.build -csv -numa -numastats -mmap -testSeconds 120 -maxNumThreads 256 -dbFilename /var/lib/bedrock/perftest.30B.db

#echo "-------------------------------------------------------------------"
#free -h
#sh -c "echo 3 > /proc/sys/vm/drop_caches"
#free -h
#./perftest -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -customQuery "SLEEP"

#echo "-------------------------------------------------------------------"
#free -h
#sh -c "echo 3 > /proc/sys/vm/drop_caches"
#free -h
#./perftest.make_SQLITE_SHARED_MAPPING -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -customQuery "NOOP"

#echo "-------------------------------------------------------------------"
#free -h
#sh -c "echo 3 > /proc/sys/vm/drop_caches"
#free -h
#./perftest.make_SQLITE_SHARED_MAPPING -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -customQuery "SELECT 1;"

#echo "-------------------------------------------------------------------"
#free -h
#sh -c "echo 3 > /proc/sys/vm/drop_caches"
#free -h
#md5sum perftest.1.db > /dev/null
#./perftest -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename perftest.1.db -customQuery "SELECT * FROM perfTest;"

#echo "-------------------------------------------------------------------"
#free -h
#sh -c "echo 3 > /proc/sys/vm/drop_caches"
#free -h
#md5sum perftest.1.db > /dev/null
#./perftest -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename perftest.1.db

#echo "-------------------------------------------------------------------"
#free -h
#sh -c "echo 3 > /proc/sys/vm/drop_caches"
#free -h
#md5sum perftest.1M.db > /dev/null
#./perftest -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename perftest.1M.db

#echo "-------------------------------------------------------------------"
#free -h
#sh -c "echo 3 > /proc/sys/vm/drop_caches"
#free -h
#md5sum perftest.10M.db > /dev/null
#./perftest -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename perftest.10M.db

#echo "-------------------------------------------------------------------"
#free -h
#sh -c "echo 3 > /proc/sys/vm/drop_caches"
#free -h
#md5sum perftest.1B.db > /dev/null
#./perftest -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename perftest.1B.db

echo "-------------------------------------------------------------------"
free -h
sh -c "echo 3 > /proc/sys/vm/drop_caches"
free -h
md5sum /var/lib/bedrock/perftest.10B.db > /dev/null
echo "./perftest.make_SQLITE_SHARED_MAPPING -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename /var/lib/bedrock/perftest.10B.db"
./perftest.make_SQLITE_SHARED_MAPPING -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename /var/lib/bedrock/perftest.10B.db

echo "-------------------------------------------------------------------"
free -h
sh -c "echo 3 > /proc/sys/vm/drop_caches"
free -h
md5sum /var/lib/bedrock/perftest.10B.db > /dev/null
echo "./perftest.make -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename /var/lib/bedrock/perftest.10B.db"
./perftest.make -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename /var/lib/bedrock/perftest.10B.db

echo "-------------------------------------------------------------------"
free -h
sh -c "echo 3 > /proc/sys/vm/drop_caches"
free -h
md5sum /var/lib/bedrock/perftest.10B.db > /dev/null
echo "./perftest.build -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename /var/lib/bedrock/perftest.10B.db"
./perftest.build -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename /var/lib/bedrock/perftest.10B.db

echo "-------------------------------------------------------------------"
free -h
sh -c "echo 3 > /proc/sys/vm/drop_caches"
free -h
md5sum perftest.10B.db > /dev/null
echo "./perftest.make_SQLITE_SHARED_MAPPING -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename perftest.10B.db"
./perftest.make_SQLITE_SHARED_MAPPING -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename perftest.10B.db

echo "-------------------------------------------------------------------"
echo "cpupower -c all set -b 7"
cpupower -c all set -b 7
free -h
sh -c "echo 3 > /proc/sys/vm/drop_caches"
free -h
md5sum /var/lib/bedrock/perftest.10B.db > /dev/null
echo "./perftest.make_SQLITE_SHARED_MAPPING -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename /var/lib/bedrock/perftest.10B.db"
./perftest.make_SQLITE_SHARED_MAPPING -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename /var/lib/bedrock/perftest.10B.db
echo "cpupower -c all set -b 0"
cpupower -c all set -b 0

echo "-------------------------------------------------------------------"
free -h
sh -c "echo 3 > /proc/sys/vm/drop_caches"
free -h
md5sum /var/lib/bedrock/perftest.30B.db > /dev/null
./perftest -csv -numa -numastats -mmap -testSeconds $TEST_SECONDS -maxNumThreads 256 -linear -dbFilename /var/lib/bedrock/perftest.30B.db

echo "==================================================================="
