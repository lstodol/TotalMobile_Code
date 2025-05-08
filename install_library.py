import json
import requests
import sys
import getopt
import time
import os


def main():
    url = ""
    token = ""
    clusterid = ""
    libspath = ""
    dbfspath = ""

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "hstcld",
            ["url=", "token=", "clusterid=", "libs=", "dbfspath="],
        )
    except getopt.GetoptError:
        print(
            "install_library.py -u <url> -t <token> -c <clusterid> -l <libs> -d <dbfspath>"
        )
        sys.exit(2)

    for opt, arg in opts:
        if opt == "-h":
            print(
                "install_library.py -u <url> -t <token> -c <clusterid> -l <libs> -d <dbfspath>"
            )
            sys.exit()
        elif opt in ("-u", "--url"):
            url = arg
        elif opt in ("-t", "--token"):
            token = arg
        elif opt in ("-c", "--clusterid"):
            clusterid = arg
        elif opt in ("-l", "--libs"):
            libspath = arg
        elif opt in ("-d", "--dbfspath"):
            dbfspath = arg

    print(
        f"""Invoked install_library.py with: 
                url:{url}
                token:{token}
                clusterid:{clusterid}
                libspath:{libspath}
                dbfspath:{dbfspath}"""
    )

    # Generate the list of files from walking the local path.
    libslist = []
    for path, subdirs, files in os.walk(libspath):
        for name in files:
            name, file_extension = os.path.splitext(name)
            if file_extension.lower() in [".whl"]:
                lib_filepath = name + file_extension.lower()
                print(f"Adding {lib_filepath} to the list of .whl files to evaluate.")
                libslist.append(lib_filepath)

    for lib in libslist:
        dbfslib = "dbfs:" + dbfspath + "/" + lib
        print(f"Evaluating whether {dbfslib}  be installed, or reinstalled.")

        status = get_lib_status(url, token, clusterid, dbfslib)
        print(f"{dbfslib} in status: {status}")
        if status is not None or status == "not found":
            print(f"{dbfslib} not found. Installing.")
            install_lib(url, token, clusterid, dbfslib)
        else:
            print(f"{dbfslib} found. Uninstalling.")
            uninstall_lib(url, token, clusterid, dbfslib)
            print(f"Restarting cluster: {clusterid}")
            restart_cluster(url, token, clusterid)
            print(f"Installing {dbfslib}.")
            install_lib(url, token, clusterid, dbfslib)


def uninstall_lib(url, token, clusterid, dbfslib):
    values = {"cluster_id": clusterid, "libraries": [{"whl": dbfslib}]}
    requests.post(
        url + "/api/2.0/libraries/uninstall",
        data=json.dumps(values),
        auth=("token", token),
    )


def restart_cluster(url, token, cluster_id):
    values = {"cluster_id": cluster_id}
    requests.post(
        url + "/api/2.0/clusters/restart",
        data=json.dumps(values),
        auth=("token", token),
    )

    p = 0
    while True:
        time.sleep(30)
        cluster_resp = requests.get(
            url + "/api/2.0/clusters/get?cluster_id=" + cluster_id,
            auth=("token", token),
        )
        clusterjson = cluster_resp.text
        jsonout = json.loads(clusterjson)
        current_state = jsonout["state"]
        print(f"{cluster_id} state is: {current_state}")
        if (
            current_state in ["TERMINATED", "RUNNING", "INTERNAL_ERROR", "SKIPPED"]
            or p >= 10
        ):
            break
        p = p + 1


def install_lib(url, token, cluster_id, dbfslib):
    values = {"cluster_id": cluster_id, "libraries": [{"whl": dbfslib}]}
    requests.post(
        url + "/api/2.0/libraries/install",
        data=json.dumps(values),
        auth=("token", token),
    )


def get_lib_status(url, token, clusterid, dbfslib):
    resp = requests.get(
        url + "/api/2.0/libraries/cluster-status?cluster_id=" + clusterid,
        auth=("token", token),
    )
    response_json = json.loads(resp.text)
    if response_json.get("library_statuses"):
        statuses = response_json["library_statuses"]

        for status in statuses:
            if status["library"].get("whl"):
                if status["library"]["whl"] == dbfslib:
                    return status["status"]
    else:
        # No libraries found.
        return "not found"


if __name__ == "__main__":
    main()
