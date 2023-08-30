# Product-Trailer
This software provides insights about the journey taken by products throughout nodes of your supply-chain network.

If you ever wondered "Where was this product sourced from?" or "Have products returned from this customer been scrapped?", then this software is what you need. To make it work, all you need is to provide an extract log of all movements registered on your system, and PT will take care of the rest.


## Framework & Usage

To run the experience:
```bash
python -m product_trailer
```

## Requirements
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
