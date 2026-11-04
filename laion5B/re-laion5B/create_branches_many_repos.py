import subprocess
import os
import tempfile


def run_command(command, cwd=None, env=None):
    try:
        result = subprocess.run(command, cwd=cwd, env=env, check=True, capture_output=True, text=True)
        print(f"Command '{' '.join(command)}' succeeded with output:\n{result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Command '{' '.join(command)}' failed with error:\n{e.stderr}")
        return None


"""
The goal of this function is to upload to a new branch in a repository, in order to later
have this branch replace the main branch while removing all previous history.

1. clone without lfs, create empty branch called "relaion" with one empty commit (or maybe a readme?), push it
2. do large upload on this branch
3. later delete the main branch, then make a new main based on relaion and pushsscript (move_to_main.py)
"""
def clone_create_branch_push(repo_name):
    cwd = os.getcwd()
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
            os.chdir(cwd)
            return
        if run_command(['git', 'rm', '-rf', '.']) is None:  # Remove all files
            os.chdir(cwd)
            return
        if run_command(['git', 'commit', '--allow-empty', '-m', 'Initial empty commit']) is None:
            os.chdir(cwd)
            return

        # Push the new branch to the remote repository
        if run_command(['git', 'push', '-u', 'origin', new_branch]) is None:
            os.chdir(cwd)
            return

        print(f"Branch '{new_branch}' created and pushed successfully.")
        os.chdir(cwd)


repo_links = [
    "laion/laion400m",
    "laion/laion400m_old",
    "laion/laion5b-h14-index",
    "laion/laion5B-index",
    "laion/laion2b-multi-vit-h-14-embeddings",
    "laion/laion1b-nolang-vit-h-14-embeddings",
    "laion/laion2b-en-vit-h-14-embeddings",
    "laion/laion2b-en-vit-l-14-embeddings",
    "laion/laion2b-multi-vit-l-14-embeddings",
    "laion/laion1b-nolang-vit-l-14-embeddings",
    "laion/laion2B-multi-md5",
    "laion/laion2B-en-md5",
    "laion/laion1B-nolang-md5",
    "laion/laion2B-multi-aesthetic",
    "laion/laion2B-en-aesthetic",
    "laion/laion1B-nolang-aesthetic",
    "laion/laion2B-multi-aesthetic-tags",
    "laion/laion2B-en-aesthetic-tags",
    "laion/laion1B-nolang-aesthetic-tags",
    "laion/laion1B-nolang-joined-translated-to-en",
    "laion/laion2B-multi-joined-translated-to-en",
    "laion/laion1b-nolang-inappropriate",
    "laion/laion2b-en-inappropriate",
    "laion/laion2b-multi-inappropriate",
    "laion/laion2B-multi-joined",
    "laion/laion1B-nolang-joined",
    "laion/laion2B-en-joined",
    "laion/laion1B-nolang-watermark",
    "laion/laion2B-multi-watermark",
    "laion/laion2B-en-watermark",
    "laion/laion-art",
    "laion/laion-coco",
    "laion/laion-pop",
    "laion/laion-30k-fid",
    "laion/laion5B-aesthetic-tags-kv",
    "laion/laion5B-watermark-safety-ordered",
    "laion/laion-high-resolution",
    "laion/laion-synthetic-115m",
    "laion/laion2B-multi-safety",
    "laion/laion2B-en-safety",
    "laion/laion1B-nolang-safety",
    "laion/aesthetics_v2_4.75",
    "laion/aesthetics_v2_4.5"
]

for repo_link in repo_links:
    clone_create_branch_push(repo_link)
    print(f"Finished creating branch of '{repo_link}'.")