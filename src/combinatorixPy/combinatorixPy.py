# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 19:07:52 2023

@author: RASULEVLAB

"""

import os
import pandas as pd
import numpy as np
import dask.array as da
from dask.distributed import Client, LocalCluster, Worker
import itertools
from time import sleep




# Load data from descriptors_file_path csv file and return numpy array of shape ( num_components, num_descriptors )
def load_descriptors_data(descriptors_file_path):
    
    try:
        # Load data from the CSV file which exclude the header as pandas dataframe
        df = pd.read_csv(descriptors_file_path, sep=','  )  
        
        # exclude the first column and first row
        df = df.iloc[:, 1:]
        
        # Convert  dataframe to  array
        descriptors  = df.values  
        
        return descriptors

    except Exception as e:
        print("Error occurred while loading descriptors CSV data:", e)
# Load data from concentrations_file_path csv file and return numpy array       
def load_concentrations_data( concentrations_file_path):
    
    try:
       
        # Load data from the CSV file which exclude the header using pandas dataframe
        df = pd.read_csv(concentrations_file_path, sep= ',' ,encoding= 'latin-1')
        
        # exclude the first column 
        df = df.iloc[:, 1:]
 
        # Convert DataFrame to  array 
        concentrations = df.values 
      
        return  concentrations

    except Exception as e:
        print("Error occurred while loading concentrations CSV data:", e)
        
        
def generate_descriptors_based_nonzero_component(descriptors_file_path, concentrations_file_path ):
    
    
    num_mixtures= get_num_mixtures(concentrations_file_path)
    num_components = get_num_components(concentrations_file_path)
    num_descriptors = get_num_descritors(descriptors_file_path)
   
    descriptors = load_descriptors_data (descriptors_file_path)
    concentrations = load_concentrations_data( concentrations_file_path)  
    
    mask = concentrations!= 0
    
    descriptors_based_nonzero_component = [descriptors [mask_row].tolist() for mask_row in mask]
    descriptors_based_nonzero_component  = np.array (descriptors_based_nonzero_component , dtype= object)
    
    # Find the maximum length of the inner arrays
    max_length = max(len(arr) for arr in descriptors_based_nonzero_component)
    
    # Pad the inner arrays with zeros to make them of equal length
    descriptors_based_nonzero_component_fixed_length = [arr + [[0] * len(descriptors_based_nonzero_component[0][0])] * (max_length - len(arr)) for arr in descriptors_based_nonzero_component]
    descriptors_based_nonzero_component_fixed_length = np.array(descriptors_based_nonzero_component_fixed_length)
    
    descriptors_based_nonzero_component_fixed_length = np.reshape(descriptors_based_nonzero_component_fixed_length ,(num_mixtures, num_components, num_descriptors ))   

    
    # Convert array to Dask array 
    descriptors_da = da.from_array(descriptors_based_nonzero_component_fixed_length, chunks="auto")
    
    return descriptors_da
    
    
                            
# Load data from concentrations_file_path csv file and return lazy dask array       
def generate_nonzero_concentrations (concentrations_file_path):
        num_mixtures = get_num_mixtures(concentrations_file_path)  
        num_components = get_num_components (concentrations_file_path)  
        
        concentrations = load_concentrations_data(concentrations_file_path)
        df = pd.DataFrame(concentrations)
        
        # Use list comprehensions to extract nonzero elements for each column
        nonzero_concentrations = [[val for val in row if val != 0] for row in df.values]
       
        
        nonzero_concentrations = np.array(nonzero_concentrations, dtype= object)
        
        
        # Find the maximum length of arrays
        max_len = max(len(arr) for arr in nonzero_concentrations)
        
        #Convert ragged array to fixed length arrya by adding zero to smaller subarrays 
        none_zero_concentrations_fixed_length = [np.pad(arr, (0, max_len - len(arr)), mode='constant') for arr in nonzero_concentrations]
        

        
        none_zero_concentrations_fixed_length = np.array(none_zero_concentrations_fixed_length)

        
        none_zero_concentrations_fixed_length =  np.reshape ( none_zero_concentrations_fixed_length, (num_mixtures, 1, num_components))      
              
    
        # Convert array to Dask array 
        concentration_da = da.from_array(none_zero_concentrations_fixed_length, chunks= "auto")
   
      
        return  concentration_da



# get the header of the csv file descriptors and repeat that num_components times in row axis and return the lazy dask array of shape ( num_components, num_descriptors )
def get_descriptor_header(descriptors_file_path, concentrations_file_path ):
    
     try:
        # Load data from the CSV file using pandas dataframe
        df = pd.read_csv(descriptors_file_path, sep = ',' ) 
        
        # Exclude the first column
        df = df.iloc[:, 1:]        
        
        # Store header of descriptor names as list size of (num_descriptors)
        header_descriptor = df.columns.tolist()
           
        # Convert list to numpy array
        header_descriptor = np.array(header_descriptor)
              
        num_components= get_num_components(concentrations_file_path)
        
        # Reapeat the header_descriptor of size (1, -1 ) vector, (num_components) times in row axis to be (num_components, -1) 
        header_descriptor = np.repeat(header_descriptor[np.newaxis, :], num_components, axis=0)
        
        #Transpose the header_descriptor  
        # header_descriptor_transpose = header_descriptor.T
        
        # Convert numpy array to lazy dask array
        header_descriptor_da = da.from_array(header_descriptor, chunks= "auto") 
        
        
        return header_descriptor_da
    
     except Exception as e:
        print("Error occurred while loading descriptor CSV data:", e) 

# Returns the  first column (num_mixtures,1) of the concentration matrix   
def get_first_column ( concentrations_file_path):
    
    try:
           
        # Load data from the second CSV file as pandas dataframe
        df = pd.read_csv(concentrations_file_path, sep=',' ,encoding='latin-1' )   
        
        
        # store mixture name column as pandas series
        first_column_mixture = df.iloc[:, 0]
        
        # convert pandas series to numpy array 
        first_column_mixture_ndarray = first_column_mixture.values
    
        num_mixture = df.shape[0]     
 
        # reshape first_column_mixture_ndarray from (num_mixtures, ) to (num_mixtures, 1)    
        column_mixtures_reshape = np.reshape(first_column_mixture_ndarray, (num_mixture, 1))
        
        # Add an element of Component/Descriptor to the begining of the array
        column_mixtures_reshape = np.insert (column_mixtures_reshape.astype(str), 0, "Component/Descriptor" )    
          
        # Resahpe the mixture_name
        mixture_name = column_mixtures_reshape.reshape(1, -1) 
        
        return mixture_name
    
    except Exception as e:
        print("Error occurred while loading concentration CSV data:", e)        


def get_num_mixtures(concentrations_file_path):
    
    try:       
        # Read the descriptors file using pandas to get the number of rows
        df = pd.read_csv(concentrations_file_path)         
        
        num_mixtutes = df.shape[0]
        
        print("num_mixtutes", num_mixtutes)
        
        return num_mixtutes
    
    except Exception as e:
        print("Error occurred while reading concentrations CSV file:", e)
        return None
  
    
    
def get_num_components(concentrations_file_path):
     
    concentrations = load_concentrations_data(concentrations_file_path)
    df = pd.DataFrame(concentrations)
    
    # Use list comprehensions to extract nonzero elements for each column
    nonzero_concentrations = [[val for val in row if val != 0] for row in df.values]
    
    nonzero_concentrations = np.array(nonzero_concentrations, dtype=object)
    
    # Find the maximum length of arrays
    max_len = max(len(arr) for arr in nonzero_concentrations)
                            
    
    return max_len


     
def get_num_descritors(descriptors_file_path):
    
    descriptors = load_descriptors_data(descriptors_file_path)
    num_descriptors = descriptors.shape[1]
    print("num_descriptors  " , num_descriptors)   

    return num_descriptors


        
def generate_combinatorial(descriptors_file_path, concentrations_file_path):    
    
   # load lazy descriptors to memory 
   descriptors =  generate_descriptors_based_nonzero_component (descriptors_file_path, concentrations_file_path )  
   
   # load lazy concentrations to memory
   concentrations = generate_nonzero_concentrations (concentrations_file_path)
   
   num_components = get_num_components (concentrations_file_path)    
   num_mixtures = get_num_mixtures(concentrations_file_path)
   
   num_descriptors = get_num_descritors(descriptors_file_path)
   
   mix_descriptors = np.power (num_descriptors, num_components ) 
   
   # you can change the chunksize  from 1e5 to 1e8 depending on datasets output and resourses
   chunk_desc =   int (mix_descriptors / (num_components * num_components)  )
   chunk_comp = num_components
   product_chunk = (chunk_desc, chunk_comp) 
   
                           
   def cartesian_product(array):
       
       array = array.astype(float)
       
       def product_mapper(x):
          
           prod = np.array(list(itertools.product(*x)))
           
           return prod
       
       cartesian_product_dask = da.map_blocks(product_mapper, array, dtype= object, chunks= product_chunk)      
              
       return cartesian_product_dask
    
   chunk_mixtures = num_mixtures 
    
   combinatorial_chunk = (chunk_mixtures, 1 , chunk_desc )
   
   # Return the combinarorial mixture descriptors of two array
   def combinatorial_descriptor(x, y):
           
                
        def combine_mapper(x, y):
            
            y = y.astype(float)
 
            result_list = []

            for i in range(num_mixtures):
                cartesian_subarray = x[i]
                concentration_row = y[i].reshape( num_components, 1 )
                result = np.dot( cartesian_subarray, concentration_row )
                result_list.append(result)
                
                
            final_result = np.array(result_list)
             
            
            final_result = np.transpose(final_result ,  (0, 2, 1) ) 
            
           
            return final_result
        
        # Map combine_mapper function blockwise
        combinatorial_da = da.map_blocks( combine_mapper, x, y, chunks= combinatorial_chunk, dtype= float )
                                   
        
        return combinatorial_da
        
                
                                                    
   # Call the cartesian product function 
   cartesian_descriptors = [cartesian_product(subarray) for subarray in descriptors]

   


   client.cluster.scale(20)
         
   sleep(5)
     
   
   # Scatter the large cartesian product list array
   cartesian_future = client.scatter(cartesian_descriptors)
 
     
   
   # Submit the concentration dask array to cluster 
   concentrations_future = client.submit (lambda x : x , concentrations)
   
   
    # Call combinatorial_descriptor (dot product) to cartesian_future and concentrations_future distributedely 
   combinatorial_future = client.submit( combinatorial_descriptor, cartesian_future, concentrations_future) 
   
  
   result = client.gather(combinatorial_future)   
   
   
   result = result.compute()

   
   return result


# Return the combination of the descriptors'name as header
def combinatorial_header(descriptors_file_path, concentrations_file_path):
    
      header_descriptor = get_descriptor_header(descriptors_file_path, concentrations_file_path)
      
      num_components = get_num_components (concentrations_file_path)  
      
      num_descriptors = get_num_descritors(descriptors_file_path)
      
      mix_descriptors = np.power (num_descriptors, num_components ) 
      
      # you can change the chunksize  from 1e5 to 1e8 depending on datasets output and resourses , 1000000
      row_chunk = int (mix_descriptors / (num_components * num_components) )
      col_chunk = num_components
      product_chunk = (row_chunk, col_chunk)
      
                
      def cartesian_product(array):
          
          def product_mapper(x):
              
             prod = np.array(list(itertools.product(*x)), dtype = object)
          
             return prod
          
          cartesian_product_dask = da.map_blocks(product_mapper, array, dtype = object, chunks= product_chunk)

          
          return cartesian_product_dask
            
      
      # Define a function to convert each element of the Cartesian product into a single comma-separated string
      def join_elements(cartesians):
          
          def join_mapper(x):
     
               join = np.array (list(map('_'.join, x)))
               join_reshaped = join [:, np.newaxis]
  
               return join_reshaped
          
          out_chunk = (row_chunk, 1)
          
          cartesian_strings = da.map_blocks (join_mapper, cartesians, dtype = object, chunks=  out_chunk)
          
          return cartesian_strings
          
      # Call the Cartesian product function  
      cartesian_header_descriptors = cartesian_product (header_descriptor)     
      
      # Scatter the large cartesian product array
      cartesian_header_future = client.scatter(cartesian_header_descriptors)     
 
       
      #  Call join_elements  to  cartesian_header_future   distributedely 
      cartesian_header_strings_future = client.submit (join_elements, cartesian_header_future)
        
      
      # Convert the future object to a local Dask arrays
      cartesian_header= client.gather(cartesian_header_strings_future)
      

      cartesian_header = da.transpose (cartesian_header)

       
      cartesian_header = cartesian_header.compute()
  
               
      return cartesian_header
 
    
    
# Function gets the dask array table and output path and write dask array to csv and returns file path dictionaty    
def write_to_csv(table_arr, output_path):
    
    
    # Convert the numpy array to pandas dataframe
    table_df = pd.DataFrame(table_arr)
    
    # Reset the index
    table_df = table_df.reset_index(drop=True)      
    
    
    # Create a separate directory for the output file
    try:
        
      # Create the output directory if it doesn't exist                                                        
       os.makedirs(output_path, exist_ok = True)     
       file_name = 'combinatorial.csv'
       file_path = os.path.join(output_path, file_name)
                
       table_df.to_csv(file_path, sep = ',', header =False, index = False ) 

       file_path_dict = {'combinatorial': file_path}

       return file_path 
   
    except Exception as e:
       print("Error occurred while writing matrices to CSV:", e)
       
       
# Function gets the descriptors and concentrations and output the result of mixture descriptors concatenated with the header mixture name and first column mixture descriptors names 
def get_result(descriptors_file_path,concentrations_file_path, output_path ,threshold_const, threshold_corr, batch_num):
    
    num_mixtures = get_num_mixtures(concentrations_file_path)
     
    descriptor_name = combinatorial_header(descriptors_file_path , concentrations_file_path  )
   
    
    mixture_names = get_first_column(concentrations_file_path).astype('object') 

    
    # transpose mixture_names to (-1, 1) 
    mixture_names = np.transpose (mixture_names) 
  
    
    
    result = generate_combinatorial(descriptors_file_path, concentrations_file_path).astype(np.dtype('float32') ) 

    
    
    result = result.reshape( num_mixtures, -1 )

    
    result = result.astype('object')
 


    concatenated = np.vstack((descriptor_name, result)) 
    
    
    concatenated_arr = np.hstack ((mixture_names, concatenated))
         
    
    # Filter mixture descriptores columns for near constant , and low pair correlation 
    filtered_constant_arr =  filter_const (concatenated_arr, threshold_const)
    filtered_highly_corr_arr = filter_high_corr_bychunck (filtered_constant_arr, threshold_corr, batch_num)
    
    # Write the dask array to csv file
    file_path = write_to_csv (filtered_highly_corr_arr, output_path)   
    
    return file_path 
  
        
 
# Remove constant and near constant combbinatorial_descriptors columns from dask array resulted from generate_combinatorial function 
def filter_const (combinatorial_descriptors_arr, threshold):
    
    # Calculate the variance of each columns
    def compute_correlation(dask_array):
        column_variance= da.var( dask_array, axis= 0)
        return column_variance
    
    combinatorial_descriptors = combinatorial_descriptors_arr [1:, 1: ].astype(float)
    combinatorial_descriptors_da = da.from_array (combinatorial_descriptors , chunks = "auto")
    combinatorial_descriptors_da = combinatorial_descriptors_da.astype(float)
    
    # Scatter the large cartesian product array
    future_combinatorial_descriptors = client.scatter(combinatorial_descriptors_da)
    
    # Callcompute_correlation the scattered Dask array
    column_variance_future = client.submit(compute_correlation, future_combinatorial_descriptors)

    
    column_variance = client.gather(column_variance_future)
   
    column_variance = column_variance.compute() 
  
    
    column_to_keep = column_variance > threshold
   
    
    column_to_keep = np.insert(column_to_keep, 0, True)
   
    
    filtered_constants_arr = combinatorial_descriptors_arr[: , column_to_keep]

    
    return filtered_constants_arr
           

       
def filter_high_corr_bychunck(combinatorial_descriptors_arr, threshold, batch_num):
        
    def highly_correlated_columns(x, threshold):
        
        correlation_matrix = np.absolute(np.corrcoef(x, rowvar = False))
        
        upper_triangle = np.triu(correlation_matrix, k= 1) 

        # to_drop = np.max(upper_triangle, axis = 1) > threshold
        highly_corr = upper_triangle > threshold
        
        keep_cols = set()
        for i in range(highly_corr.shape[0]):
            for j in range(i+1, highly_corr.shape[1]):
                if highly_corr[i,j]:
                    keep_cols.add(i)
                    
        return keep_cols       
    
    combinatorial_descriptors = combinatorial_descriptors_arr [1: , 1: ].astype(float)  
   
    
    num_mixture = combinatorial_descriptors.shape[0]
 
    mix_descriptors = combinatorial_descriptors.shape[1]
 
   
    columns_to_keep_result = []
    
    batch_size = int ( mix_descriptors / batch_num )

    # Process the matrix in batches
    for start in range(0, mix_descriptors, batch_size):
        end = min(start + batch_size, mix_descriptors)
        
        # Extract the batch of columns
        batch = combinatorial_descriptors [:, start: end]  
        
        keep_col = highly_correlated_columns (batch, threshold_corr)
   
        
        keep_col = np.array(list(keep_col))
      
        keep_col += start
        columns_to_keep_result.append ( keep_col.tolist() )
    
        
    
    if len(columns_to_keep_result) > 0:        
        
        columns_to_keep = np.concatenate(columns_to_keep_result) 

        
        
        columns_to_keep += 1
    
        
        matrix_high_corr = combinatorial_descriptors_arr [:, columns_to_keep ] 

    else: 
        
        columns_to_keep = np.array([], dtype = bool)
     
        matrix_high_corr = combinatorial_descriptors_arr 

     
    return matrix_high_corr

def initialize_dask_cluster(config=None):
    """
    Initializes and returns a Dask LocalCluster based on the provided configuration.
    
    Parameters:
    - config (dict): Configuration dictionary for Dask LocalCluster. Expected keys:
        - scheduler_address (str): Address of the Dask scheduler. If provided, connects to the existing scheduler.
        - n_workers (int): Number of workers to use (only if creating a LocalCluster).
        - threads_per_worker (int): Number of threads per worker (only if creating a LocalCluster).
        - memory_limit (str): Memory limit per worker (only if creating a LocalCluster).
        - Additional keyword arguments for LocalCluster.
    
    Returns:
    - LocalCluster: A Dask LocalCluster instance or a connection to an existing scheduler.
    """
    if config is None:
        config = {}
    
    scheduler_address = config.get('scheduler_address', None)
    n_workers = config.get('n_workers', 1)
    threads_per_worker = config.get('threads_per_worker', 1)
    memory_limit = config.get('memory_limit', '2GB')
    timeout = config.get('timeout' , 300)
    
    if scheduler_address:
        # Connect to an existing Dask scheduler
        cluster = Client(scheduler_address).cluster
    else:
        # Create a LocalCluster
        cluster = LocalCluster(
            n_workers=n_workers, 
            threads_per_worker=threads_per_worker, 
            memory_limit=memory_limit,
            timeout= timeout
            
        )
    
    return cluster

