#! /bin/bash
# This is shell script could repeat the tests,
# generator log file and raft tmp file
# usage:
# modify following args
#   count: repeat times
#   test_test: makefile's test, you can modify makefile to add subtest
# In the root of this project, run `./shell/tinytest.sh`
# If some error occur, try `chmod a+x ./shell/tinytest.sh`
# And also, you can run `make cleanlog` to delete all logs

count=1
test_test=project2a


deleteTmp() {
    # echo "deleting tinykv rubish"
    find /tmp/ -name "*raft*" | xargs rm -rf -v
    # echo "deleting finished"
}

i=0
while(( $i < $count))
do
    echo "round `expr $i + 1`"
    if [ $test_test ]; then
        make $tinytetest_testst_test > $test_test-$i.log
        deleteTmp
    else
        break
    fi
    let "i++"
    
done
