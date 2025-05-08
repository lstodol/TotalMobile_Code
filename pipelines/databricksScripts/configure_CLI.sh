#!/bin/sh
url=$1
global_jobs_config=$2

echo 'Configuring Databricks CLI and generating PAT token'
access_token=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --output json| jq -r .accessToken)
echo "##vso[task.setvariable variable=accesstoken;]$access_token"

auth_header="Authorization: Bearer $access_token"
echo "##vso[task.setvariable variable=authheader;]$auth_header"

pat_token_config=$(jq -n -c --arg ls "3600" --arg co "DevOps Token" '{lifetime_seconds: ($ls|tonumber), comment: $co}')
pat_token_response=$(echo "$pat_token_config" | curl -sS -X POST -H "$auth_header" --data-binary "@-" "https://$url/api/2.0/token/create")

pat_token=`echo $pat_token_response | jq -r .token_value`
echo "##vso[task.setvariable variable=pattoken;]$pat_token"

curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh > ~/dbx_log.txt
echo "[DEFAULT]
host  = https://$url
token = $pat_token" > ~/.databrickscfg

cluster_name=$(jq -r '.ClusterName' $global_jobs_config)

cluster_info=$(databricks clusters list | grep -i $cluster_name | sort)
if [[ ! -z ${cluster_info} ]] && [[ $(echo $cluster_info | wc -l) = 1 ]];  then
    cluster_id=${cluster_info:0:20}
    echo "Cluster exists with ID: $cluster_id"
    echo "##vso[task.setvariable variable=clusterid;]$cluster_id"
else
    echo "No cluster to run test on it."
    exit 1
fi