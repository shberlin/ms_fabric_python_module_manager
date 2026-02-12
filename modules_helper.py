import os
import sys
import time
import shutil
import base64
import requests
import tempfile
import importlib
import notebookutils
import concurrent.futures
from typing import Dict, List
from sempy.fabric import FabricRestClient


REST_CLIENT = FabricRestClient()
dummy_call = REST_CLIENT.get(f"v1/workspaces")
BASE_URL = dummy_call.url[:-13]
CURRENT_WS = notebookutils.runtime.context["currentWorkspaceName"]
CURRENT_WS_ID = notebookutils.runtime.context["currentWorkspaceId"]
HEADERS = dummy_call.request.headers
TEMPDIR = os.path.join(tempfile.gettempdir(), "python_modules", CURRENT_WS)


def _api_request(method: str, api_path: str, body: dict = None, interval: int = 1):
    """
    Make an API Request.
    """
    if body != None:
        if method.upper() == "POST":
            response = requests.post(BASE_URL + api_path, json = body, headers = HEADERS)
        else:
            raise Exception("Only method POST is supported with a body")
    else:
        response = REST_CLIENT.request(method, api_path)

    if response.status_code == 202:
        lro_path = response.headers["Location"]

        while lro_path:
            time.sleep(interval) # lower interval than the original sempy request with LRO_wait = True
            response = REST_CLIENT.request("GET", lro_path)
            lro_path = response.headers.get("Location")
            
            if lro_path and lro_path.endswith("/result"):
                response = REST_CLIENT.request("GET", lro_path)
                return response.json()            

    return response.json()


def _get_module_notebooks(relevant_folders: List[str]=None) -> List[Dict[str, str]]:
    """
    Get module notebooks in current workspace.
    """
    notebooks = _api_request("GET", f"v1/workspaces/{CURRENT_WS_ID}/items?type=Notebook")
    notebooks = notebooks["value"]

    if relevant_folders is None:
        # No filtering needed, return all notebooks
        return notebooks

    folders = _api_request("GET", f"v1/workspaces/{CURRENT_WS_ID}/folders")
    relevant_folder_ids = []
    for folder in folders["value"]:
        if folder["displayName"] in relevant_folders:
            relevant_folder_ids.append(folder["id"])

    filtered_notebooks = [n for n in notebooks if n.get("folderId") in relevant_folder_ids]
    return filtered_notebooks


def _download_module_notebook_to_temp_folder(module_notebook: Dict[str, str]):
    """
    Download the notebook content of a module notebook and save it as a .py file in the temporary folder.
    """

    notebook_name = module_notebook["displayName"]
    notebook_id = module_notebook["id"]
    script_name = f"{notebook_name}.py"

    # Notebookutils take more than 20 seconds.
    # definition = notebookutils.notebook.getDefinition(notebook_name, workspace_id)
    # The direct rest api call is much faster
    # Thanks goes out to Gerhard Brueckl:
    # https://blog.gbrueckl.at/2025/06/using-custom-libraries-in-microsoft-fabric-data-engineering/
    notebook_definition = _api_request("POST", f"v1/workspaces/{CURRENT_WS_ID}/items/{notebook_id}/getDefinition?format=fabricGitSource")

    if notebook_definition.get("status", "") == "Failed":
        raise Exception(f"Failed to get notebook definition for {notebook_name}, status: {notebook_definition['status']}, response: {notebook_definition}")

    notebook_part = [part for part in notebook_definition["definition"]["parts"] if part["path"].startswith("notebook-content")][0]
    file_content = base64.b64decode(notebook_part["payload"]).decode("utf-8")

    local_file = os.path.join(TEMPDIR, script_name)
    with open(local_file, "w", encoding="utf-8") as f:
        f.write(file_content)


def refresh_modules(relevant_folders: List[str]=["lib"], notebooks_to_ignore: List[str]=[]):
    """
    Refresh the module notebooks by downloading them to a temporary folder.
    """

    if os.path.isdir(TEMPDIR):
        shutil.rmtree(TEMPDIR)
    os.makedirs(TEMPDIR, exist_ok=True)

    if notebooks_to_ignore is None:
        notebooks_to_ignore = []

    module_notebooks = _get_module_notebooks(relevant_folders=relevant_folders)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Downloading the module notebooks cuncurrently is much faster than sequencial processing
        future_to_url = {
            executor.submit(_download_module_notebook_to_temp_folder, module_notebook):
            module_notebook["displayName"] for module_notebook in module_notebooks
            if module_notebook["displayName"] not in notebooks_to_ignore
        }
        for future in concurrent.futures.as_completed(future_to_url):
            notebook_name = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (notebook_name, exc))
                raise exc

    # Move TEMPDIR to the front in sys.path
    if TEMPDIR not in sys.path:
        sys.path.insert(0, TEMPDIR)

    # Drop python modules from sys.modules to have a fresh state
    importlib.invalidate_caches()
    for module_notebook in module_notebooks:
        notebook_name = module_notebook["displayName"]
        sys.modules.pop(notebook_name, None)
