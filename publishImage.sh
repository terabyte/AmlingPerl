#!/bin/sh

for dir in bin libs
do
    mkdir -p image/"$dir"
    rsync -av --progress --delete "$dir"/ image/"$dir"/
done
