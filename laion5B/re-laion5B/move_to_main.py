
"""
3. delete the main branch, then make a new main based on relaion and pushs
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

def replace_main_with_relaion(repo_name):
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

        # Change the current working directory to the cloned repository
        os.chdir(clone_dir)

        # Create a new empty branch and an empty commit
        if run_command(['git', 'checkout',  'relaion']) is None:
            return
        if run_command(['git', 'branch', '-D', 'main']) is None:  # Remove all files
            return
        if run_command(['git', 'checkout', '-b', 'main']) is None:
            return
        if run_command(['git', 'push', '-u', 'origin', '--force', 'main']) is None:
            return
        
        print(f"Branch 'main' replaced with 'relaion' and pushed successfully.")

replace_main_with_relaion("laion/my-test-repo")