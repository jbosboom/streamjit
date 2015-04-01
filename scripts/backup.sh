#!/bin/bash
#Author - Sumanan
#Feb 25, 2015
#Backups tuning output files and directories.
args=("$@")
suffix=${args[0]}
if [ -z $suffix ]
then
suffix="Orig"
fi

if [ -d summary$suffix ]; then
    echo "summary$suffix exists. No backups. Exiting..."
    exit
fi

mv summary summary$suffix
mv compileTime.txt compileTime$suffix.txt
mv runTime.txt runTime$suffix.txt
mv drainTime.txt drainTime$suffix.txt
mv GraphProperty.txt GraphProperty$suffix.txt
mv README.txt README$suffix.txt

