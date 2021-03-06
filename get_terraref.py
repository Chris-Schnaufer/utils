#!/usr/bin/env python3
""" Generate TERRA REF canopy cover """

import argparse
import logging
import os
import stat
import subprocess
from typing import Optional

import globus_sdk

GLOBUS_ENDPOINT = 'Terraref'
GLOBUS_PATH = '/ua-mac/public/season-6/Level_2/rgb_fullfield/'
LOCAL_SAVE_PATH = os.path.realpath(os.getcwd())
GLOBUS_LOCAL_ENDPOINT_ID = None
GLOBUS_CLIENT_ID = '80e3a80b-0e81-43b0-84df-125ce5ad6088'  # This script's ID registered with Globus
IRODS_LOCATION = '/iplant/home/schnaufer/terraref'


def globus_get_authorizer() -> globus_sdk.RefreshTokenAuthorizer:
    """Returns Globus authorization information (requires user interaction)
    Return:
        The authorizer instance
    """
    auth_client = globus_sdk.NativeAppAuthClient(GLOBUS_CLIENT_ID)
    auth_client.oauth2_start_flow(refresh_tokens=True)

    authorize_url = auth_client.oauth2_get_authorize_url()
    print("Authorization URL: %s" % authorize_url)
    print("Go to the following URL to obtain the authorization code:", authorize_url)

    get_input = getattr(__builtins__, 'raw_input', input)
    auth_code = get_input('Enter the authorization code: ').strip()

    token_response = auth_client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_info = token_response.by_resource_server['transfer.api.globus.org']

    return globus_sdk.RefreshTokenAuthorizer(transfer_info['refresh_token'], auth_client,
                                             access_token=transfer_info['access_token'],
                                             expires_at=transfer_info['expires_at_seconds'])


def globus_download_files(client: globus_sdk.TransferClient, endpoint_id: str, files: tuple) -> None:
    """Gets the details of the files in the list
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        files: the list of files to fetch
    Return:
        Returns an updated list of file details
    """
    # Fetch metadata and pull information out of it
    file_transfers = {}
    for one_file in files:
        globus_save_path = os.path.join(LOCAL_SAVE_PATH, os.path.basename(one_file))
        if not os.path.exists(globus_save_path):
            globus_remote_path = one_file
            file_transfers[globus_remote_path] = globus_save_path

    if file_transfers:
        have_exception = False
        cnt = 1

        resp = subprocess.run(['icd', IRODS_LOCATION], stdout=subprocess.PIPE)
        if resp.returncode != 0:
            raise RuntimeError("Unable to change to iRODS location %s" % IRODS_LOCATION)

        for remote_path, save_path in file_transfers.items():
            try:
                logging.info("Trying transfer %s: %s", str(cnt), str(remote_path))
                cnt += 1
                transfer_setup = globus_sdk.TransferData(client, endpoint_id, GLOBUS_LOCAL_ENDPOINT_ID,
                                                         label="Get image file", sync_level="checksum")
                transfer_setup.add_item(remote_path, save_path)
                transfer_request = client.submit_transfer(transfer_setup)
                task_result = client.task_wait(transfer_request['task_id'], timeout=600, polling_interval=5)
                if not task_result:
                    raise RuntimeError("Unable to retrieve file: %s" % remote_path)
                if not os.path.exists(save_path):
                    raise RuntimeError("Unable to find downloaded file at: %s" % save_path)

                local_dir = os.getcwd()
                os.chdir(os.path.dirname(save_path))
                print("Uploading file to irods: %s", save_path)
                resp = subprocess.run(['iput', '-K', '-f', os.path.basename(save_path)], stdout=subprocess.PIPE)
                if resp.returncode != 0:
                    os.chdir(local_dir)
                    raise RuntimeError("Unable to load file to iRODS %s" % save_path)
                os.chdir(local_dir)
                print("    removing uploaded file")
                os.remove(save_path)

            except RuntimeError as ex:
                have_exception = True
                logging.warning("Failed to get image: %s", str(ex))
        if have_exception:
            raise RuntimeError("Unable to retrieve all files individually")
        del file_transfers


def query_files(client: globus_sdk.TransferClient, endpoint_id: str, folders: tuple, extensions: tuple,
                exclude_parts: tuple) -> Optional[tuple]:
    """Returns a list of files on the endpoint path that match the dates provided
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        folders: a list of folders to search within (search is 1 deep)
        extensions: a list of acceptable filename extensions (can be wildcard '*')
    Return:
        Returns a list of acceptable files with the extension(s)
    """
    get_input = getattr(__builtins__, 'raw_input', input)

    found_files = []
    check_ext = [e.lstrip('.') for e in extensions]
    out_file = open(os.path.join(LOCAL_SAVE_PATH, 'file_list.txt'), 'w')
    for one_folder in folders:
        cur_path = os.path.join('/-', one_folder)
        logging.debug("Globus files path: %s", cur_path)
        try:
            path_contents = client.operation_ls(endpoint_id, path=cur_path)
        except globus_sdk.exc.TransferAPIError:
            logging.error("Continuing after TransferAPIError Exception caught for: '%s'", cur_path)
            continue

        matches = []
        for one_entry in path_contents:
            if one_entry['type'] != 'dir':
                file_path = os.path.join(cur_path, one_entry['name'])
                logging.debug("Globus remote file path: %s", file_path)

                # Get the format of the file (aka: its extension)
                file_format = os.path.splitext(one_entry['name'])[1]
                if file_format:
                    file_format = file_format.lstrip('.')

                # Check if it's included
                if file_format not in check_ext:
                    logging.debug("   remote file doesn't match extension: %s %s", os.path.basename(file_path), check_ext)
                    continue

                if exclude_parts:
                    found_exclude = False
                    for part in exclude_parts:
                        if part in one_entry['name']:
                            found_exclude = True
                            break
                    if found_exclude:
                        logging.warning("  remote file name includes an excluded term: %s %s", one_entry['name'], exclude_parts)
                        continue

                matches.append(file_path)

        if matches:
            done = False
            while not done:
                print("Remote folder", one_folder)
                print("Please select file to download:")
                print(0, ".", "None")
                idx = 1
                for one_match in matches:
                    print(idx, ".", os.path.basename(one_match))
                    idx += 1
                sel_file = get_input('Enter the number associated with file: ').strip()
                sel_idx = int(sel_file)
                if sel_idx > 0:
                    if sel_idx <= len(matches):
                        logging.debug(" file index %s selected", sel_idx)
                        found_files.append(matches[sel_idx - 1])
                        out_file.write(matches[sel_idx - 1] + '\n')
                        done = True
                    else:
                        print("Entered value is out of range: %s %d", sel_file, sel_idx)
                elif sel_idx == 0:
                    print("Skipping folder")
                    done = True
                else:
                    print("Invalid entry")
                if not done:
                    print("Please try again")
            print("-")
            print("-")

    out_file.close()
    print("Done searching for files to download: found", len(found_files), "files")

    return tuple(found_files)


def globus_get_folders(client: globus_sdk.TransferClient, endpoint_id: str, remote_path: str) -> Optional[tuple]:
    """Returns a list of files on the endpoint path that match the dates provided
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        remote_path: the remote path to search
    Return:
        Returns a list of found sub folders
    """
    base_path = os.path.join('/-', remote_path)
    return_paths = []
    try:
        path_contents = client.operation_ls(endpoint_id, path=base_path)
    except globus_sdk.exc.TransferAPIError:
        logging.error("Continuing after TransferAPIError Exception caught for: '%s'", base_path)
        return None

    for one_entry in path_contents:
        if one_entry['type'] == 'dir':
            sub_folder = os.path.join(base_path, one_entry['name'])
            logging.debug("Globus remote sub folder: %s", sub_folder)
            return_paths.append(sub_folder)

    return tuple(return_paths)


def globus_get_tif_files(globus_authorizer: globus_sdk.RefreshTokenAuthorizer, remote_endpoint: str,
                         remote_path: str, filepath_download: str = None) -> None:
    """Fetches files in the remote folder
    Arguments:
        globus_authorizer: the Globus authorization instance
        remote_endpoint: the remote endpoint to access
        remote_path: the path of remote folder to start in
        filepath_download: path to file containing the list of files to download
    """
    # Prepare to fetch file information from Globus
    trans_client = globus_sdk.TransferClient(authorizer=globus_authorizer)

    # Find the remote ID
    endpoint_id = None
    for endpoint in trans_client.endpoint_search(filter_scope='shared-with-me'):
        if 'display_name' in endpoint and endpoint['display_name'] == remote_endpoint:
            endpoint_id = endpoint['id']
            break
        if 'canonical_name' in endpoint and endpoint['canonical_name'] == remote_endpoint:
            endpoint_id = endpoint['id']
            break
    if not endpoint_id:
        raise RuntimeError("Unable to find remote endpoint: %s" % remote_endpoint)

    # Get all the sub folders for this location
    folders = globus_get_folders(trans_client, endpoint_id, remote_path)

    # Query for all the files to download
    files = query_files(trans_client, endpoint_id, folders, ('.tif', '.TIF', '.tiff', '.TIFF'),
                        ('_10pct', '_thumb', '_copy', '_mask', '_nrmac', 'test'))

    # Download the files
    globus_download_files(trans_client, endpoint_id, files)


def generate() -> None:
    """Performs all the steps needed to generate the SQLite database
    Exceptions:
        RuntimeError exceptions are raised when something goes wrong
    """
    global GLOBUS_LOCAL_ENDPOINT_ID

    logging.getLogger().setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='Download files using Globus and upload to IRODS')
    parser.add_argument('--list', type=str, help='Filename containing the the list of files to download')

    args = parser.parse_args()

    # Make sure our storage endpoint exists
    if not os.path.exists(LOCAL_SAVE_PATH):
        os.makedirs(LOCAL_SAVE_PATH, exist_ok=True)
        os.chmod(LOCAL_SAVE_PATH, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IWGRP|stat.S_IXGRP|
                 stat.S_IROTH|stat.S_IWOTH|stat.S_IXOTH)

    resp = subprocess.run(['globus', 'endpoint', 'local-id'], stdout=subprocess.PIPE)
    if resp.returncode != 0:
        raise RuntimeError("Unable to get Local Endpoint ID for Globus. Please use --globus_local_endpoint_id and try again")
    GLOBUS_LOCAL_ENDPOINT_ID = resp.stdout.decode('ascii').rstrip('\n')

    # Get the Globus authorization
    authorizer = globus_get_authorizer()

    # Create the files table
    globus_get_tif_files(authorizer, GLOBUS_ENDPOINT, GLOBUS_PATH, args.list)


if __name__ == "__main__":
    generate()
