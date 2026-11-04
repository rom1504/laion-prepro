
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
    cwd = os.getcwd()
    # Define the repository URL
    repo_url = f'https://huggingface.co/datasets/{repo_name}'
    
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as clone_dir:
        print(f"Cloning into temporary directory: {clone_dir}")

        # Clone the repository without Git LFS
        env = os.environ.copy()
        env['GIT_LFS_SKIP_SMUDGE'] = '1'

        clone_command = ['git', 'clone', '--single-branch', '--branch', "relaion", repo_url, clone_dir]
        if run_command(clone_command, env=env) is None:
            return

        # Change the current working directory to the cloned repository
        os.chdir(clone_dir)

        # Create a new empty branch and an empty commit
        if run_command(['git', 'checkout',  'relaion']) is None:
            os.chdir(cwd)
            return
        #if run_command(['git', 'branch', '-D', 'main']) is None:  # Remove all files
        #    return
        if run_command(['git', 'checkout', '-b', 'main']) is None:
            os.chdir(cwd)
            return
        if run_command(['git', 'push', '-u', 'origin', '--force', 'main']) is None:
            os.chdir(cwd)
            return
        
        print(f"Branch 'main' replaced with 'relaion' and pushed successfully.")
        os.chdir(cwd)

#replace_main_with_relaion("laion/laion-coco")


to_process = [
    {
        "repo":"laion/laion400m",
        "type": "en",
        "input_folder": "/media/hd2/backup/laion400m",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion400m",
        "compute": False,
    },
    {
        "repo":"laion/laion400m_old", # field URL
        "type": "en",
        "input_folder": "/media/hd2/backup/laion400m_old",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion400m_old",
    },
    {
        "repo": "laion/laion2B-multi-aesthetic",# field URL
        "type": "multi",
        "input_folder": "/media/hd2/backup/laion2B-multi-aesthetic",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion2B-multi-aesthetic",
    },
    {
        "repo": "laion/laion2B-en-aesthetic",
        "type": "en",
        "input_folder": "/media/hd2/backup/laion2B-en-aesthetic",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion2B-en-aesthetic",
    },
    {
        "repo": "laion/laion1B-nolang-aesthetic",
        "type": "nolang",
        "input_folder": "/media/hd2/backup/laion1B-nolang-aesthetic",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion1B-nolang-aesthetic",
    },
    {
        "repo": "laion/laion1B-nolang-joined-translated-to-en", # field URL
        "type": "nolang",
        "input_folder": "/media/hd2/backup/laion1B-nolang-joined-translated-to-en",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion1B-nolang-joined-translated-to-en",
    },
    {
        "repo": "laion/laion2B-multi-joined-translated-to-en",
        "type": "multi",
        "input_folder": "/media/hd2/backup/laion2B-multi-joined-translated-to-en",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion2B-multi-joined-translated-to-en",
    },
    {
        "repo": "laion/laion-art", # field URL
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-art",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-art",
    },
    {
        "repo": "laion/laion-coco", # field URL
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-coco",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-coco",
    },
    {
        "repo": "laion/laion-pop",
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-pop",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-pop",
    },
    {
        "repo": "laion/laion-high-resolution", # URL
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-high-resolution",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-high-resolution",
    },
    {
        "repo": "laion/laion-synthetic-115m",
        "type": "en",
        "input_folder": "/media/hd2/backup/laion-synthetic-115m",
        "output_folder": "/media/hd2/relaion/variants_relaion_research_safe/laion-synthetic-115m",
    },
]

to_process = [
    {
        "repo": "laion/relaion2B-en-research-safe"
    },
    {
        "repo": "laion/relaion2B-multi-research-safe"
    },
    {
        "repo": "laion/relaion1B-nolang-research-safe"
    }
]

to_process = [
    {"repo": "laion/aesthetics_v2_4.5"},
    {"repo": "laion/aesthetics_v2_4.75"},
]

for item in to_process:
    replace_main_with_relaion(item["repo"])