#!/bin/bash

###################################################################################
#
# DATE:     06-21-2016
# AUTHOR:   RAJAN SHAH
# PURPOSE:  Collect Bloggers articles
# LICENSE:	IntelliMind LLC
# 
# Base Dir: $1 
# Config:  $2
# Root Dir: /var/feed/data/bloggers - $3
#
###################################################################################

if [ $# -ne 3 ]; then
    echo "Usage:blogger_stories.sh <base_dir> <config> <root_dir>"
    exit 0
else
    echo "blogger_stories.sh $@"
fi

# set virtual environment
if [[ $VIRTUAL_ENV && ${VIRTUAL_ENV-x} ]]; then
    echo "Environment:", $VIRTUAL_ENV
    source $VIRTUAL_ENV/bin/activate
fi

directory_name="/tmp/newsfeed"
if [ -d $directory_name ]
then
    echo "Directory already existing"
else
    mkdir -p $directory_name
fi

export BASE_DIR=$1
export SRC_DIR=$BASE_DIR/influencer
export FEED_CONFIG=$2
export DATA_DIR=$3
export LOG_DIR=$directory_name


echo python $SRC_DIR/pipeline_extract.py --root_dir=$DATA_DIR --config=$FEED_CONFIG 

python $SRC_DIR/pipeline_extract.py --root_dir=$DATA_DIR --config=$FEED_CONFIG > $LOG_DIR/pipeline_extract.log 2>&1 
