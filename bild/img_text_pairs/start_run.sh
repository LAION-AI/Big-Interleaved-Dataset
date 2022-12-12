#!/bin/bash
#SBATCH --partition=g80n54
#SBATCH --nodes 1
#SBATCH --gpus 8
#SBATCH --comment laion
#SBATCH --output=%x_%j.out
#SBATCH --exclusive
#SBATCH --job-name=bild_img_txt_pairs

# HOSTNAMES MASTER_ADDR MASTER_PORT COUNT_NODE are coming from the main script
#export LD_LIBRARY_PATH=/usr/local/cuda-11.6/lib:/usr/local/cuda-11.6/lib64:$LD_LIBRARY_PATH
#export PATH=/usr/local/cuda-11.6/bin:$PATH

echo myuser=`whoami`
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
echo PATH=$PATH
echo HOSTNAMES=$HOSTNAMES
echo hostname=`hostname`

# ARGS
CONVERT="False"
DOWNLOAD_IMGS="False"
MODEL_TYPE="sentence_transformers"
MATCHING_THRESHOLD=0.28
ENABLE_WANDB="True"
MAX_BATCH_SIZE=16384
echo CONVERT=$CONVERT
echo DOWNLOAD_IMGS=$DOWNLOAD_IMGS
echo MODEL_TYPE=$MODEL_TYPE
echo MATCHING_THRESHOLD=$MATCHING_THRESHOLD
echo ENABLE_WANDB=$ENABLE_WANDB
echo MAX_BATCH_SIZE=$MAX_BATCH_SIZE
# ARGS

source /admin/home-siddhesh1793/.env/bin/activate
echo python3 version = `python3 --version`
python -c "import torch; print (torch.__version__)"

python run.py --convert $CONVERT --download_imgs $DOWNLOAD_IMGS --model_type $MODEL_TYPE --matching_threshold $MATCHING_THRESHOLD --enable_wandb $ENABLE_WANDB --max_batch_size $MAX_BATCH_SIZE
