# Product-Trailer
This software provides insights about the journey taken by products throughout nodes of your supply-chain network.

If you ever wondered "Where was this product sourced from?" or "Have products returned from this customer been scrapped?", then this software is what you need. To make it work, all you need is to provide an extract log of all movements registered on your system, and PT will take care of the rest.

Network diagram generated:
![Network diagram generated](/assets/Example_Network_diagram.png)  


## Framework & Usage

To run the software: `python main.py *your_profile_name*`

Get help:
```bash
...\Product_Trailer> python main.py -h
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

## Profiles

Your profile is what defines:  
* What is the format of the input data ("movements data")
* How to pre-process it to align with the algorithm
* Which signal to take, in order to start tracking products
* The output report you want to get from it: The default Excel output? With more details? Graphs? You decide!
* Many other parameters, such as the directory where to store reports, filenames, etc.

Profiles are stored under ./profiles/*your_profile_name*/
  
For a quick-start, just run the command `python main.py *your_profile_name*`. The program will automatically create your profile and process the movements files detected.

To allow the program to work with your data, you will likely need to customize the preprocessing steps. To do so, copy the *default_profile* and edit the .py files. They have been documentated to facilitate your work.


(Additional documentation about typical usage methodology, program architecture, screenshots to be added soon)


## Output
By default, output files are recorded at the root of your profile: ./profiles/your_profile_name/

Example output:
![Example output](/assets/Example_output.png)  
An example output file is available in /assets/


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
- python=3.11
- tqdm
- pytest
- line_profiler
- networkx
- matplotlib
- xlsx2csv


## Contributing

You found this repo and you like it? Feel free to get in touch or to raise an issue!  
Pull requests welcome. For major changes, please open an issue first to discuss what you would like to change.  
You could get involved by...
- Using different input format, and increasing speed of Excel imports
- Increase overall performance (speed)
- Propose different output format, including graphical representations
