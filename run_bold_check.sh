#!/bin/bash
#BSUB -J bioscan_bold_check
#BSUB -o /lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results/bold_check_%J.log
#BSUB -e /lustre/scratch126/tol/teams/lawniczak/users/lp20/bioscan_plate_checker_results/bold_check_%J.err
#BSUB -n 1
#BSUB -M 8000
#BSUB -R "span[hosts=1] select[mem>8000] rusage[mem=8000]"
#BSUB -W 2:00

. /usr/share/modules/init/bash
module load HGI/softpack/users/aw43/BOLDconnectR_bioscan/2

# Load API key from .env file (never hardcode)
if [ -f ~/bioscan_plate_checker/.env ]; then
    export $(cat ~/bioscan_plate_checker/.env | xargs)
fi

cd ~/bioscan_plate_checker
Rscript bold_check.R "$@"
