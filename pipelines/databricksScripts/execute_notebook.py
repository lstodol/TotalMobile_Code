import json
import requests
import sys
import getopt
import time


def main():
    url = ""
    token = ""
    clusterid = ""
    build_id = ""
    notebook_path = ""

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "hs:t:c:lwo",
            [
                "url=",
                "token=",
                "clusterid=",
                "buildid=",
                "notebook_path=",
            ],
        )
    except getopt.GetoptError:
        print(
            "executenotebook.py -u <url> -t <token>  -c <clusterid> -b <buildid> -n <notebook_path>)"
        )
        sys.exit(2)

    for opt, arg in opts:
        if opt == "-h":
            print(
                "executenotebook.py -u <url> -t <token> -c <clusterid> -b <buildid> -n <notebook_path>"
            )
            sys.exit()
        elif opt in ("-u", "--url"):
            url = arg
        elif opt in ("-t", "--token"):
            token = arg
        elif opt in ("-c", "--clusterid"):
            clusterid = arg
        elif opt in ("-b", "--buildid"):
            build_id = arg
        elif opt in ("-n", "--notebook_path"):
            notebook_path = arg

    print(
        f"""Invoked execute_notebook.py with: 
                url:{url}
                token:{token}
                clusterid:{clusterid}
                buildid:{build_id}
                notebook_path:{notebook_path}"""
    )

    values = {
        "run_name": "DevOpsRunner",
        "existing_cluster_id": clusterid,
        "timeout_seconds": 3600,
        "notebook_task": {   
            "notebook_path": notebook_path,
            "base_parameters": {
                "run_id": build_id,
            },
        },
    }

    response = requests.post(
        url + "/api/2.0/jobs/runs/submit",
        data=json.dumps(values),
        auth=("token", token),
    )
    
    print(f"response from submitted run: {response.text}")
    run_id = json.loads(response.text)["run_id"]

    i = 0
    while True:
        time.sleep(15)
        response_get = requests.get(
            url + "/api/2.0/jobs/runs/get?run_id=" + str(run_id),
            data=json.dumps(values),
            auth=("token", token),
        )
        print(f"response from get by run id: {response_get.text}")
        
        state = json.loads(response_get.text)["state"]
        current_state = state["life_cycle_state"]
        if current_state in ["TERMINATED", "INTERNAL_ERROR", "SKIPPED"] or i >= 120:
            if state["result_state"] != "SUCCESS":
                raise Exception("Notebook has not been successfully executed.")
            break
        i = i + 1
            

        
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        sys.exit(1)