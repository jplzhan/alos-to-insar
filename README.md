# alos-to-insar
Notebook PGE for running INSAR on ALOS-1 data downloaded from ASF.

# Initializing `run.py`, a wrapper for Otello Commands
`run.py` is a wrapper for using the Otello API to submit jobs to the PCM cluster. To use it, you need to install Otello and initialize it.

To start off, clone the Otello repository using the following command:
```
git clone https://github.com/hysds/otello.git
```
Then install it. I recommend creating a local Conda environment by running the following commands in your Otello folder:
```
cd otello/
conda create --prefix .conda python=3.9
conda activate .conda/
pip install -e .
```
Now you need to initialize Otello using the following step, entering the HySDS host as `https://` followed by the corresponding IP (depends on OnDemand system) and your username. `HySDS cluster authenticated (y/n)` can be declined with `n`:
```
$ otello init
HySDS host (current value: https://###.##.###.###/):
Username (current value: ########):
HySDS cluster authenticated (y/n): n

auth: false
host: https://###.##.###.###/
username: ########
```
Now copy `run.py` to your Otello folder.

# Running `run.py`
- Activate the Otello Conda environment (if not already done)
- Login to `aws-login.linux.amd64`.
- Configure ALOS-1 datasets for `run.py` and verify they are in the relative `data/` folder or on S3 already.
- Run `run.py`.
