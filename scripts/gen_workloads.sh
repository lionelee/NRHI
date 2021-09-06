#!/bin/bash

script_path=$(realpath $(dirname $0))
proj_path=${script_path%/*}
ycsb_dir=$proj_path/YCSB
data_dir=$proj_path/data
workloads_dir=$proj_path/workloads
workloads=(a b c)
types=(micro macro)

if [ ! -d $data_dir ]
then
    mkdir $data_dir
fi

for t in ${types[@]}
do
    ddir=$data_dir/$t
    if [ ! -d $ddir ]
    then
        mkdir $ddir
    fi

    for w in ${workloads[@]}
    do
        $ycsb_dir/bin/ycsb.sh load basic -P $workloads_dir/workloada -P $workloads_dir/$t.dat > $ddir/load$w.dat
        $ycsb_dir/bin/ycsb.sh run basic -P $workloads_dir/workload$w -P $workloads_dir/$t.dat > $ddir/run$w.dat

        linenum_load=`cat $ddir/load$w.dat | wc -l`
        linenum_load_last=`expr $linenum_load - 23`
        linenum_run=`cat $ddir/run$w.dat | wc -l`
        linenum_run_last=`expr $linenum_run - 30`
        sed -i '1,17d;'$linenum_load_last','$linenum_load'd;' $ddir/load$w.dat
        sed -i '1,17d;'$linenum_run_last','$linenum_run'd;' $ddir/run$w.dat
    done
done

cp $data_dir/micro/loada.dat $data_dir/micro/updatea.dat
sed -i 's/INSERT/UPDATE/g' $data_dir/micro/updatea.dat
cp $data_dir/micro/loada.dat $data_dir/micro/deletea.dat
sed -i 's/INSERT/DELETE/g' $data_dir/micro/deletea.dat