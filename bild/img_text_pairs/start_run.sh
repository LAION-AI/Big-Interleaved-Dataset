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

source /admin/home-siddhesh1793/.env/bin/activate
echo python3 version = `python3 --version`
python -c "import torch; print (torch.__version__)"

python run.py --filename /admin/home-siddhesh1793/data/00000.parquet