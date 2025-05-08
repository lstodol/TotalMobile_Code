#!/bin/sh
workflow_json=$1
global_jobs_config=$2

echo "Creating a Job with definition from '$workflow_json'" 

recipients_array=($(jq -r '.NotificationRecipients[]' $global_jobs_config))
json_recipients_array=$(printf '%s\n' "${recipients_array[@]}" | jq -R . | jq -s .)
jobTimeoutThreshold=$(jq -r '.JobTimeoutThreshold' $global_jobs_config)
jobDurationThreshold=$(jq -r '.JobDurationThreshold' $global_jobs_config)
clusterName=$(jq -r '.ClusterName' $global_jobs_config)

cluster_info=$(databricks clusters list | grep -i $clusterName)
if [[ ! -z ${cluster_info} ]] && [[ $(echo $cluster_info | wc -l) = 1 ]];  then
    cluster_id=${cluster_info:0:20}
    echo "Cluster exists with ID: $cluster_id"
    echo "##vso[task.setvariable variable=clusterid;]$cluster_id"
else
    echo "No cluster to run job on it."
    exit 1
fi

job_name=$(jq -r '.name' $workflow_json)
job_id=$(databricks jobs list | grep $job_name | awk '{print $1}')

echo "params for create or update the job : 
        notificationRecipients:$json_recipients_array, 
        jobTimeoutThreshold:$jobTimeoutThreshold, 
        jobDurationThreshold:$jobDurationThreshold, 
        clusterName:$clusterName,
        clusterId:$cluster_id,
        job_name:$job_name,
        job_id:$job_id"


jq --argjson json_recipients_array "$json_recipients_array" \
    '.email_notifications.on_failure = $json_recipients_array'  $workflow_json \
    > tmp.json && mv tmp.json $workflow_json
jq --argjson json_recipients_array "$json_recipients_array" \
    '.email_notifications.on_duration_warning_threshold_exceeded = $json_recipients_array'  $workflow_json  \
    > tmp.json && mv tmp.json $workflow_json
jq --argjson jobTimeoutThreshold "$jobTimeoutThreshold" \
    '.timeout_seconds = $jobTimeoutThreshold'  $workflow_json  \
    > tmp.json && mv tmp.json $workflow_json
jq --argjson jobDurationThreshold "$jobDurationThreshold" \
    '.health.rules |=  map(if .metric == "RUN_DURATION_SECONDS" then .value = $jobDurationThreshold else . end)'  $workflow_json  \
    > tmp.json && mv tmp.json $workflow_json
jq --arg clusterName "$clusterName" \
    'walk(if type == "object" and has("cluster_name") then .cluster_name = $clusterName else . end)'  $workflow_json  \
    > tmp.json && mv tmp.json $workflow_json
jq --arg cluster_id "$cluster_id" \
    'walk(if type == "object" and has("existing_cluster_id") then .existing_cluster_id = $cluster_id else . end)'  $workflow_json  \
    > tmp.json && mv tmp.json $workflow_json

if [[ -z "$job_id" ]]; then
   echo "Creating the job."
   cat $workflow_json
   databricks jobs create --json @$workflow_json
else
   echo "Updating the job."
   jq -n --argjson job_id "$job_id" '{job_id : $job_id}'  > update_tmp.json 
   jq -s '.[0] + {new_settings: .[1]}' update_tmp.json  $workflow_json > tmp.json && mv tmp.json $workflow_json
   cat $workflow_json
   databricks jobs update --json @$workflow_json
fi