#!/bin/bash
#Author - Sumanan
# A sample file to download tuning files from Lanka cluster.
app=FMRadio15
mainClass=FMRadioCore
if [ -d $app ]; then
	echo "$app exists. No downloads..."
 	exit
fi
mkdir -p $app
cd $app
mkdir -p $mainClass
scp -r sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app/$mainClass/\{summary,*.txt,streamgraph.dot\} $mainClass/
scp -r sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app/\{*.sh,slurm-*,options.properties,*.jar\} .
	
