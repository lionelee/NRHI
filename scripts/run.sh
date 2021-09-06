#!/bin/bash

script_path=$(realpath $(dirname $0))
proj_path=${script_path%/*}
test_dir=$proj_path/build/tests
data_dir=$proj_path/data
res_dir=$proj_path/results

if [ ! -d $res_dir ]
then
    mkdir $res_dir
fi

types=(micro macro)
workloads=(a b c)
indexes=(clht cceh cmap clevel nrhi)

for i in {1..3}
do 
    for t in ${types[@]}
    do
        ddir=$data_dir/$t
        if [ ! -d $res_dir/$t ]
        then
            mkdir $res_dir/$t
        fi
        for w in ${workloads[@]}
        do
            if [ ! -d $res_dir/$t/$w ]
            then
                mkdir $res_dir/$t/$w
            fi
            for index in ${indexes[@]}
            do
                numactl --cpunodebind=1 \
                $test_dir/$index'_test_ycsb_'$t /mnt/pmem1/hashpool $ddir/load$w.dat $ddir/run$w.dat 36

                mv $index'_latency.res' $index'_latency_'$i'.res'
                mv $index'_throughput.res' $index'_throughput_'$i'.res'
                mv *.res $res_dir/$t/$w

                sleep 10
            done
        done
    done
done
