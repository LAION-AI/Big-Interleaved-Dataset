#!/bin/bash
#SBATCH --partition=g80n54
#SBATCH --nodes 1
#SBATCH --gpus 8
#SBATCH --comment laion
#SBATCH --output=%x_%j.out
#SBATCH --exclusive
#SBATCH --job-name=bild_img_txt_pairs

# HOSTNAMES MASTER_ADDR MASTER_PORT COUNT_NODE are coming from the main script

echo myuser=`whoami`
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
echo PATH=$PATH
echo HOSTNAMES=$HOSTNAMES
echo hostname=`hostname`

# ARGS
CONVERT="False"
echo CONVERT=$CONVERT

DOWNLOAD_IMGS="False"
echo DOWNLOAD_IMGS=$DOWNLOAD_IMGS

MODEL_TYPE="open_clip"
echo MODEL_TYPE=$MODEL_TYPE

MATCHING_THRESHOLD=0.3
echo MATCHING_THRESHOLD=$MATCHING_THRESHOLD

ENABLE_WANDB="True"
echo ENABLE_WANDB=$ENABLE_WANDB

MAX_BATCH_SIZE=16384
echo MAX_BATCH_SIZE=$MAX_BATCH_SIZE

FILTER_BY_LANG="True"
echo FILTER_BY_LANG=$FILTER_BY_LANG

LOG_FREQUENCY=1000
echo LOG_FREQUENCY=$LOG_FREQUENCY
# ARGS

source /admin/home-siddhesh1793/.env/bin/activate
echo python3 version = `python3 --version`
python -c "import torch; print (torch.__version__)"

python run.py --convert $CONVERT \
				--download_imgs $DOWNLOAD_IMGS \
				--model_type $MODEL_TYPE \
				--matching_threshold $MATCHING_THRESHOLD \
				--enable_wandb $ENABLE_WANDB \
				--max_batch_size $MAX_BATCH_SIZE \
				--filter_by_lang $FILTER_BY_LANG \
				--log_frequency $LOG_FREQUENCY