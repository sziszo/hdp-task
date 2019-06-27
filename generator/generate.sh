#!/usr/bin/env bash

if  [[ "$#" -ne 2 ]]; then
    echo "Usage generate.sh <size> <output-folder> ";
    exit 1;
fi

size=$1;
output_dir=$2;

if ((${size} <= 0)); then
    echo "The size must be greater than 0!";
    exit 1;
fi

echo -n "Checking output directory.."
hdfs dfs -test -e ${output_dir}
echo -n "."
if [[ $? == 0 ]]; then
    echo -ne "\n"
    read -p "The output directory exists! Do you wish to delete it? [y/n]: " answer;
    case ${answer} in
        [Yy]* ) hdfs dfs -rm -f -r ${output_dir};;
        * ) exit 1;;
    esac
else echo -ne " ok!\n";
fi

echo -n "Preparing input directory.."

input_dir="generator";
hdfs dfs -test -e ${input_dir}
echo -n "."
if [[ $? != 0 ]]; then
    hdfs dfs -mkdir -p ${input_dir}
fi

echo -n "."
names=${input_dir}/names.txt
cities=${input_dir}/cities.txt
hdfs dfs -copyFromLocal -f names.txt ${names}
echo -n "."
hdfs dfs -copyFromLocal -f cities.txt ${cities}
echo -ne " done!\n"

hadoop jar generator-1.0-SNAPSHOT.jar com.example.GeneratorDriver ${size} ${output_dir} ${names} ${cities}
