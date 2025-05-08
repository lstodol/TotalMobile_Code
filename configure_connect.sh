#!/bin/sh
url=$1
pat_token=$2
cluster_id=$3
dbx_id=$4

echo 'Configuring Databricks Connect'

echo "Params for databricks connect
    https://$url
    $pat_token
    $cluster_id
    $dbx_id
    15001"

echo "y
https://$url
$pat_token
$cluster_id
$dbx_id
15001" | databricks-connect configure
databricks-connect test