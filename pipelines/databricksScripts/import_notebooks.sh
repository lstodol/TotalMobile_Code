#!/bin/sh
notebooks_path=$1
auth_header=$2
url=$3
workspace_path=$4

$echo "Invoked import_notebook.sh with params: notebooks_path:$notebooks_path, auth_header:$auth_header, url:$url"

echo "Searching for notebooks in $notebooks_path"
for f in $(find $notebooks_path -name '*.py' -or -name '*.ipynb');\
do 
    base_dir=$(dirname ${f})
    file=$(basename ${f})
    file="${file%.*}"
    dbx_dir=${base_dir//${notebooks_path:1}/${workspace_path}}
    dbx_filepath="$dbx_dir"/"$file"
    echo "Importing [${base_dir}] [${file}] to [$dbx_filepath]"
    databricks workspace mkdirs $dbx_dir
    content=$(base64 -w 0 $f)
    payload=$( jq -n \
        --arg p "$dbx_filepath" \
        --arg c "$content" \
        '{path: $p, content: $c, language: "PYTHON", overwrite: true, format: "SOURCE"}' )
    echo "$payload" | curl -sS -X POST -H "$auth_header" --data-binary "@-" "https://$url/api/2.0/workspace/import"
done;


