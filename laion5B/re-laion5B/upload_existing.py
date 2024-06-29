from huggingface_hub import HfApi
from huggingface_hub.commands.large_upload import large_upload

"""
The goal of this script is to upload to a new branch in a repository, in order to later
have this branch replace the main branch while removing all previous history.

1. clone without lfs, create empty branch called "relaion" with one empty commit (or maybe a readme?), push it
2. do large upload on this branch
3. later delete the main branch, then make a new main based on relaion and pushs
"""

import subprocess
import os
import tempfile
from pathlib import Path

def run_command(command, cwd=None, env=None):
    try:
        result = subprocess.run(command, cwd=cwd, env=env, check=True, capture_output=True, text=True)
        print(f"Command '{' '.join(command)}' succeeded with output:\n{result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Command '{' '.join(command)}' failed with error:\n{e.stderr}")
        return None

def clone_create_branch_push(repo_name):
    # Define the repository URL
    repo_url = f'https://huggingface.co/datasets/{repo_name}'
    
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as clone_dir:
        print(f"Cloning into temporary directory: {clone_dir}")

        # Clone the repository without Git LFS
        env = os.environ.copy()
        env['GIT_LFS_SKIP_SMUDGE'] = '1'

        clone_command = ['git', 'clone', repo_url, clone_dir]
        if run_command(clone_command, env=env) is None:
            return

        # Define the new branch name
        new_branch = 'relaion'

        # Change the current working directory to the cloned repository
        os.chdir(clone_dir)

        # Create a new empty branch and an empty commit
        if run_command(['git', 'checkout', '--orphan', new_branch]) is None:
            return
        if run_command(['git', 'rm', '-rf', '.']) is None:  # Remove all files
            return
        if run_command(['git', 'commit', '--allow-empty', '-m', 'Initial empty commit']) is None:
            return

        # Push the new branch to the remote repository
        if run_command(['git', 'push', '-u', 'origin', new_branch]) is None:
            return

        print(f"Branch '{new_branch}' created and pushed successfully.")

def upload(repo_name, folder_path):
    # Create an API object
    api = HfApi()

    print(f"Uploading large files in '{folder_path}' to repository '{repo_name}'.")
    # Perform the large upload
    large_upload(
        folder_path=folder_path,
        repo_id=repo_name,
        repo_type="dataset",
        api=api,
        revision='relaion',
    )

def create_branch_then_upload(repo_name, folder_path):
    clone_create_branch_push(repo_name)
    upload(repo_name, folder_path)

create_branch_then_upload('laion/my-test-repo', "/media/hd2/relaion_research/relaion1B-nolang-research")


