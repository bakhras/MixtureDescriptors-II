#CombinatorixPy: Mixture Descriptors Calculator
This Python project calculates an combinatorial scheme for mixture descriptors of multi-component materials. The algorithm processes input from two `.csv` files and computes combinatorial mixture descriptors. This code computes combinatorial mixture descriptors based on cartesian product which is cartesian product of N sets of $D_1 \times D_2 \times \dots \times D_N$ belonging to constituents' descriptors of each mixture, while N is the number of components.


## Overview
CombinatorixPy is a Python package designed to generate combinatorial mixture descriptors for multi-component materials. It processes input data from two CSV files to compute these descriptors, facilitating advanced analysis in materials science.

## Features

- **Descriptor Generation**: Creates mixture descriptors using a combinatorial approach based on Cartesian product, which is the Cartesian product of N sets of $D_1 \times D_2 \times \dots \times D_N$ belonging to constituents' descriptors of each mixture, where N is the number of components.
- **Data Processing**: Handles input from CSV files containing individual descriptors and mole fraction values for each component in mixtures.
- **Filtering**: Supports filtering of constant and nearly constant descriptors, as well as highly correlated pairs, with adjustable thresholds.


The algorithm requires two input files:

1. Descriptors File: Contains individual descriptors for each component.
2. Mole Fraction File: Contains mole fraction values for each component in each mixture.
The main function, mixture_descriptors_to_csv, processes these inputs and generates a CSV file. 


## Getting Started

### Prerequisites

Ensure you have Python 3.x installed. You will also need `pip` to install the package.

### Installation
You can install the package in two different ways depending on whether you want to install from the local directory or directly from PyPI:

Option 1: Install from Local Directory
   1. **Download the Package:**
      - Clone the repository or download the ZIP file from GitHub.
      - Extract the contents of the ZIP file.

   2. **Install the Package:**
      - Open a command-line interface (CLI).
      - Navigate to the directory containing the extracted package files.
      - Run the following command to install the package:

      ```bash
      pip install .
      ```
Option 2: Install from PyPI
If you prefer to install the package directly from PyPI https://pypi.org/project/combinatorixPy/. You can use the following command, Be sure to replace 1.0.1 with the latest version number as updates become available:  

   ```bash
   pip install combinatorixPy
   ```

## Usage

After installing the package, you can use it in your Python code. Here’s a basic example of how to use the main function:


```python

from combinatorixPy import initialize_dask_cluster, Usageget_result
# initialize the cluster 
config = {
        'n_workers': 1,
        'threads_per_worker': 30,
        'memory_limit': '100GB',
        'timeout': 300
    }
    
    
    # Initialize the Dask client, dask_cluster with the provided config
    cluster = initialize_dask_cluster(config)
    client = Client(cluster)

# Call the function
get_result(
    descriptors_file_path='path/to/descriptors.csv',
    mole_fraction_file_path='path/to/mole_fractions.csv',
    output_directory='path/to/output',
    constant_threshold=0.01,
    correlation_threshold=0.9,
    batch_number=100000, 
    client
)
```
OR

```python
from combinatorixPy import initialize_dask_cluster, Usageget_result
# Configuration for connecting to an existing scheduler
config = {
    'scheduler_address': 'tcp://localhost:8786'  # Replace with your scheduler's address
}
# Initialize the Dask client with the provided config
    cluster = initialize_dask_cluster(config)
    client = Client(cluster)

# Call the function
get_result(
    descriptors_file_path='path/to/descriptors.csv',
    mole_fraction_file_path='path/to/mole_fractions.csv',
    output_directory='path/to/output',
    constant_threshold=0.01,
    correlation_threshold=0.9,
    batch_number=100000, 
    client
)


```

## Arguments
After installation, you can use the package's functions in your Python code. The main function get_result requires seven arguments:  
1- descriptors_file_path: Path to the CSV file containing individual descriptors for each component.  
2- mole_fraction_file_path: Path to the CSV file with mole fraction values for each component in each mixture.  
3- output_directory: Directory path where the resulting CSV file will be saved.  
4- constant_threshold: Threshold for filtering out constant and nearly constant descriptors.  
5- correlation_threshold: Threshold for removing highly correlated descriptor pairs.  
6- batch_number: Batch number for processing highly correlated pairs due to large correlation matrices.  
7- client: An instance of a dask.distributed.Client  

   
## License
This project is licensed under the GNU General Public License - see the LICENSE file for details.


