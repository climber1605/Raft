#!/bin/bash
# This shell script has to be put in the same folder with raft.go.
cd ../../
export GOPATH=$PWD
cd src/raft/

times=100
declare -a tests=(
    "InitialElection"
    "ReElection"
    "BasicAgree"
    "FailNoAgree"
    "ConcurrentStarts"
    "Rejoin"
    "Backup"
    "Persist1"
    "Persist2"
    "Persist3"
    "FailAgree"
    "Count"
    "Figure8"
    "UnreliableAgree"
    "Figure8Unreliable"
    "ReliableChurn"
    "UnreliableChurn" 
)

# Test Figure8 (Unreliable) for 100 times. Total running time is about 1 hour.
#for ((i=1; i<=$times; i++)); do
#    go test -run "^TestFigure8Unreliable$"| tee temp.txt
#    state=$(tail -n 2 temp.txt | head -n 1)
#    if [ "$state" != "PASS" ]; then 
#        echo "Fail to pass test Figure8 (Unreliable) after "$i" times tests"
#        rm -f temp.txt
#        exit 1
#    else
#    echo "Pass test Figure8 (Unreliable) "$i" times"
#    fi
#done
#echo "Pass test Figure8 (Unreliable) after "$times" times tests" 

# Test all the 17 tests declared in test_test.go 100 times each. Total running time is about 4.5 hours.
for test in ${tests[@]}; do
    for ((i=1; i<=$times; i++)); do
        go test -run "^Test"$test"$" | tee temp.txt
        state=$(tail -n 2 temp.txt | head -n 1)
        if [ "$state" != "PASS" ]; then 
            echo "Fail to pass test "$test" after "$i" times tests"
            rm -f temp.txt
            exit 1
        else
            echo "Pass test "$test" "$i" times"
        fi
    done
    echo "Pass test "$test" after "$times" times tests" 
done
echo "Pass all the tests after "$times" times tests" 

# Test all the 17 tests declared in test_test.go 100 times each in data race detecting mode. Total running time is about 4.5 hours.
for ((i=1; i<=$times; i++)); do
    go test -race | tee temp.txt
    state=$(tail -n 2 temp.txt | head -n 1)
    if [ "$state" != "PASS" ]; then 
        echo "Fail to pass all the tests in race mode after "$i" times tests"
        rm -f temp.txt
        exit 1
    else
    echo "Pass all the tests in race mode "$i" times"
    fi
done
echo "Pass all the tests in race mode after "$times" times tests" 

rm -f ./temp.txt