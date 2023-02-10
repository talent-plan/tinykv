#!/bin/sh

make $1 &> make.log
grep FAIL make.log
if [[ $? -ne 0 ]]; then
    rm make.log
    exit 0
else
    rm make.log
    exit 1
fi
