# 1. read datasets
# 2. anti join with broadcast
# 3. shuffle, repartition
# 4. write datasets
# 5. upload to huggingface hub

from pyspark.sql import SparkSession
from  pyspark.sql.functions import rand
from huggingface_hub import HfApi
from huggingface_hub.commands.large_upload import large_upload
import uuid
import shutil
import subprocess
import os
import tempfile

# mapping from previous naming
# research-safe -> research
# safe -> research-safe
# do this rename in other files

root_tmp_path="/media/nvme/root_tmp_path"

def create_spark_session():
    spark = SparkSession.builder.config("spark.driver.memory", "16G").config("spark.executor.memory", "1G").config("spark.storage.decommission.enabled", "true").config("spark.storage.cleanupFilesAfterExecutorExit", "true").config("spark.sql.shuffle.partitions", "1000") .config("spark.local.dir", "/media/nvme/spark-tmp").master("local[16]").appName('spark-stats').getOrCreate()
    return spark


def write_temp_read_recreate_spark(spark, df, root_tmp_path):
    tmp_path=root_tmp_path+"/"+uuid.uuid4().hex
    df.write.mode("overwrite").parquet(tmp_path)
    output_df = spark.read.parquet(tmp_path)
    return [spark, output_df, tmp_path]



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

def create_branch_then_upload(repo_id, folder_path):
    clone_create_branch_push(repo_id)
    upload(repo_id, folder_path)

# create_branch_then_upload('laion/my-test-repo', "/media/hd2/relaion_research/relaion1B-nolang-research")

def filter_send(input_en,
                input_multi,
                input_nolang,
                safer,
                output_path_en,
                output_path_multi,
                output_path_nolang,
                repo_id_en,
                repo_id_multi,
                repo_id_nolang,
                compute,
                upload):
    
    if compute:
        spark = create_spark_session()
        en = spark.read.parquet(input_en + "/*.parquet")
        multi = spark.read.parquet(input_multi + "/*.parquet")
        nolang = spark.read.parquet(input_nolang + "/*.parquet")

        if not safer:
            all_unsafe_en = spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-en-md5-unsafe")
            all_unsafe_multi = spark.read.parquet("/media/hd2/relaion_research_unsafe/laion2B-multi-md5-unsafe")
            all_unsafe_nolang = spark.read.parquet("/media/hd2/relaion_research_unsafe/laion1B-nolang-md5-unsafe")
        else:
            all_unsafe_en = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-en-md5-safe_unsafe_merged")
            all_unsafe_multi = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion2B-multi-md5-safe_unsafe_merged")
            all_unsafe_nolang = spark.read.parquet("/media/hd2/relaion_safe_unsafe_merged/laion1B-nolang-md5-safe_unsafe_merged")

        [spark, pre_en, tmp_path_en] = write_temp_read_recreate_spark(spark, en.join(all_unsafe_en, ["url"], "leftanti"), root_tmp_path)
        en_filtered = pre_en.orderBy(rand()).repartition(128)
        en_filtered.write.mode("overwrite").parquet(output_path_en)
        shutil.rmtree(tmp_path_en)
        [spark, pre_multi, tmp_path_multi] = write_temp_read_recreate_spark(spark, multi.join(all_unsafe_multi, ["url"], "leftanti"), root_tmp_path)
        multi_filtered = pre_multi.orderBy(rand()).repartition(128)
        multi_filtered.write.mode("overwrite").parquet(output_path_multi)
        shutil.rmtree(tmp_path_multi)
        [spark, pre_nolang, tmp_path_nolang] = write_temp_read_recreate_spark(spark, nolang.join(all_unsafe_nolang, ["url"], "leftanti"), root_tmp_path)
        nolang_filtered = pre_nolang.orderBy(rand()).repartition(128)
        nolang_filtered.write.mode("overwrite").parquet(output_path_nolang)
        shutil.rmtree(tmp_path_nolang)


    if upload:
        create_branch_then_upload(
            folder_path=output_path_en,
            repo_id=repo_id_en,
        )
        create_branch_then_upload(
            folder_path=output_path_multi,
            repo_id=repo_id_multi,
        )
        create_branch_then_upload(
            folder_path=output_path_nolang,
            repo_id=repo_id_nolang,
        )

# relaion-research
filter_send(input_en="/media/hd2/backup/laion2B-en-md5",
            input_multi="/media/hd2/backup/laion2B-multi-md5",
            input_nolang="/media/hd2/backup/laion1B-nolang-md5",
            safer=False,
            output_path_en="/media/hd2/relaion_research/relaion2B-en-research",
            output_path_multi="/media/hd2/relaion_research/relaion2B-multi-research",
            output_path_nolang="/media/hd2/relaion_research/relaion1B-nolang-research",
            repo_id_en="laion/relaion2B-en-research",
            repo_id_multi="laion/relaion2B-multi-research",
            repo_id_nolang="laion/relaion1B-nolang-research",
            compute=False,
            upload=False)

# relaion-research-safe = main relaion5B
filter_send(input_en="/media/hd2/backup/laion2B-en-md5",
            input_multi="/media/hd2/backup/laion2B-multi-md5",
            input_nolang="/media/hd2/backup/laion1B-nolang-md5",
            safer=True,
            output_path_en="/media/hd2/relaion_research_safe/relaion2B-en-research-safe",
            output_path_multi="/media/hd2/relaion_research_safe/relaion2B-multi-research-safe",
            output_path_nolang="/media/hd2/relaion_research_safe/relaion1B-nolang-research-safe",
            repo_id_en="laion/laion2B-en",
            repo_id_multi="laion/laion2B-multi",
            repo_id_nolang="laion/laion1B-nolang",
            compute=False,
            upload=False)