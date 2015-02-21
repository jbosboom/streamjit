#!/bin/bash
#Author - Sumanan
#Feb 9, 2015
#Setup directories and scripts to run a distributed StreamJit app.
function writeRun(){
	runfile="run.sh"
	res=$(get_prop "./options.properties" "tune")
	echo "#!/bin/bash" > $runfile
	echo "#SBATCH --tasks-per-node=1" >> $runfile
	echo "#SBATCH -N 1"  >> $runfile
	echo "#SBATCH --cpu_bind=verbose,cores" >> $runfile
	echo "#SBATCH --exclusive" >> $runfile
	echo "cd /data/scratch/sumanan/"$1 >> $runfile
	if [ "$res" -eq "1" ];then
		echo "mkdir -p $2" >> $runfile
		echo "cd $2" >> $runfile
		echo "srun python ../lib/opentuner/streamjit/streamjit2.py 12563 &" >> $runfile
		echo "cd .." >> $runfile
	fi
	echo "srun -l ../bin/java/jdk1.8.0_25/bin/java -Xmx2048m -jar $1.jar $3" >> $runfile
}

function writeSN(){
	runfile="streamnode.sh"
	echo "#!/bin/bash" > $runfile
	echo "#SBATCH --tasks-per-node=1" >> $runfile
	echo "#SBATCH -N $2"  >> $runfile
	echo "#SBATCH --cpu_bind=verbose,cores" >> $runfile
	echo "#SBATCH --exclusive" >> $runfile
	echo "cd /data/scratch/sumanan/"$1 >> $runfile
	echo "srun --exclusive  --nodes=$2 ../bin/java/jdk1.8.0_25/bin/java -Xmx2048m -jar StreamNode.jar 128.30.116." >> $runfile
}

function creatdirs(){
	mkdir -p $1
	ln -s /data/scratch/sumanan/data $1/data
	ln -s /data/scratch/sumanan/lib $1/lib
	cd $1
}

get_prop(){
	grep  "^${2}=" ${1}| sed "s%${2}=\(.*\)%\1%"
}

if [ "$#" -ne 3 ]; then
	echo "Illegal number of parameters"
	echo "3 arguments must be passed"
	echo "setup.sh <app> <mainClass> <noOfnodes>"
	exit
fi

args=("$@")
app=${args[0]}
mainClass=${args[1]}
nodes=${args[2]}
totalNodes=$((nodes + 1))
cd /data/scratch/sumanan
creatdirs $app
writeRun $app $mainClass $totalNodes
writeSN $app $nodes
