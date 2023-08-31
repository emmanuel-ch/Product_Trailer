# Product-Trailer
This software provides insights about the journey taken by products throughout nodes of your supply-chain network.

If you ever wondered "Where was this product sourced from?" or "Have products returned from this customer been scrapped?", then this software is what you need. To make it work, all you need is to provide an extract log of all movements registered on your system, and PT will take care of the rest.


## Framework & Usage

To run the software: `python -m product_trailer return_analysis`

Get help:
```bash
...\Product_Trailer> python -m product_trailer -h
usage: Product-Trailer [-h] [-r RAW_DIR] [-p RAW_PREFIX] [-ne] profile_name

Tracking products through supply-chain network by using product movement logs.

positional arguments:
  profile_name

options:
  -h, --help            show this help message and exit
  -r RAW_DIR, --raw-dir RAW_DIR
  -p RAW_PREFIX, --raw-prefix RAW_PREFIX
  -ne, --no-excel-report
  ```

(Additional documentation about typical usage methodology, program architecture, screenshots to be added soon)


## Output
By default, output files are recorded at the root of your profile: ./profiles/your_profile_name/

Example output:
![Example output](/assets/Example_output.png)  
An example output file is available at in /assets/


## Installation and setup

### Step by step
1. Clone the repository `git clone https://github.com/emmanuel-ch/Product_Trailer.git`
2. Setup your environment
    * By copying the environment: `conda env create -n protrail -f env.yml`
    * Or any other mean - see requirements below

3. Optional: Tune your profile  
A profile allows you to configure the input files, the processing, the output.
If no profile exists, the default one will be copied and used.  
Profiles are stored under ./profiles/
4. You're ready to go!

### Requirements
Developed on Python 3.11. 
Conda environment can be imported from env.yml.
Non-standard packages used:
- openpyxl
- numpy
- pandas
- tqdm
- pytest


## Contributing

You found this repo and you like it? Feel free to get in touch or to raise an issue!  
Pull requests welcome. For major changes, please open an issue first to discuss what you would like to change.  
You could get involved by...
- Using different input format, and increasing speed of Excel imports
- Increase overall performance (speed)
- Propose different output format, including graphical representations
