#!/bin/bash
# Build lambda packages  #
set -e

BUILD_DIR='./build'
SRC_DIR='./lambdas'
echo "Building lambda deployment packages..."

# Check-Links

function packagePythonLambda {
    proj=$1

    echo "Building " $proj

    mkdir -p $BUILD_DIR/$proj
    # Add python requirements
    pip install --upgrade -r $SRC_DIR/$proj/requirements.txt -t $BUILD_DIR/$proj
    # Copy lambda src
    cp $SRC_DIR/$proj/$proj\_runner.py $BUILD_DIR/$proj/
    # Zip it
    cd $BUILD_DIR/$proj/ && zip -r ../$proj.py.zip ./* && cd -
    # Delete working dir
    rm -r $BUILD_DIR/$proj
}

packagePythonLambda 'store_crawler_results'
packagePythonLambda 'check_links'
packagePythonLambda 'analytics_worker'
