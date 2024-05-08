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
        print ( "descriptors dataframe shape ", df.shape)
        
        # Convert  dataframe to  array
        descriptors  = df.values
        print("descriptors type ", type(descriptors))  
        print("descriptors size ", descriptors.size)     
        
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
        
        # print("concentrations ", df)
        print ( "concentrations dataframe shape ", df.shape)      
        
        # Convert DataFrame to  array 
        concentrations = df.values
        print("concentrations type ", type(concentrations))   
        print("concentrations shape ", concentrations.shape)  
      
        return  concentrations

    except Exception as e:
        print("Error occurred while loading concentrations CSV data:", e)
        
        
def generate_descriptors_based_nonzero_component(descriptors_file_path, concentrations_file_path ):
    
    
    num_mixtures= get_num_mixtures(concentrations_file_path)
    num_components = get_num_components(concentrations_file_path)
    num_descriptors = get_num_descritors(descriptors_file_path)
   
    descriptors = load_descriptors_data (descriptors_file_path)
    concentrations = load_concentrations_data( concentrations_file_path)
    
    print("descriptors type ", descriptors.shape)   
    print("concentrations shape ", concentrations.shape)
    
    mask = concentrations!= 0
    print("mask shape ", mask.shape)
    
    descriptors_based_nonzero_component = [descriptors [mask_row].tolist() for mask_row in mask]
    descriptors_based_nonzero_component  = np.array (descriptors_based_nonzero_component , dtype= object)
    
    # Find the maximum length of the inner arrays
    max_length = max(len(arr) for arr in descriptors_based_nonzero_component)
    
    # Pad the inner arrays with zeros to make them of equal length
    descriptors_based_nonzero_component_fixed_length = [arr + [[0] * len(descriptors_based_nonzero_component[0][0])] * (max_length - len(arr)) for arr in descriptors_based_nonzero_component]
    descriptors_based_nonzero_component_fixed_length = np.array(descriptors_based_nonzero_component_fixed_length)
    
    descriptors_based_nonzero_component_fixed_length = np.reshape(descriptors_based_nonzero_component_fixed_length ,(num_mixtures, num_components, num_descriptors ))   
    print("descriptors_based_nonzero_component_fixed_length shape :  ", descriptors_based_nonzero_component_fixed_length.shape) 
    
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
        print ( "nonzero_concentrations  ", nonzero_concentrations)
        
        nonzero_concentrations = np.array(nonzero_concentrations, dtype= object)
        
        print ( "nonzero_concentrations numpy array  ", nonzero_concentrations)
        
        # Find the maximum length of arrays
        max_len = max(len(arr) for arr in nonzero_concentrations)
        
        #Convert ragged array to fixed length arrya by adding zero to smaller subarrays 
        none_zero_concentrations_fixed_length = [np.pad(arr, (0, max_len - len(arr)), mode='constant') for arr in nonzero_concentrations]
        
        print ( "none_zero_concentrations_fixed_length  ", none_zero_concentrations_fixed_length)
        
        none_zero_concentrations_fixed_length = np.array(none_zero_concentrations_fixed_length)
        print ( "none_zero_concentrations_fixed_length numpy array  ", none_zero_concentrations_fixed_length)
        print("none_zero_concentrations_fixed_length numpy array shape :  ", none_zero_concentrations_fixed_length.shape) 
        
        none_zero_concentrations_fixed_length =  np.reshape ( none_zero_concentrations_fixed_length, (num_mixtures, 1, num_components))      
            
        print ( "nonzero_concentrations with zero padded  ", nonzero_concentrations)         
        print("none_zero_concentrations_fixed_length shape :  ", none_zero_concentrations_fixed_length.shape)    
    
        # Convert array to Dask array 
        concentration_da = da.from_array(none_zero_concentrations_fixed_length, chunks= "auto")
        print("concentration_da type ", type(concentration_da))   
        print("concentration_da shape ", concentration_da.shape)      
      
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
        print("header_descriptor_da type ", type(header_descriptor_da))                        
        print("header_descriptor_da shape " , header_descriptor_da.shape)  
        
        
        return header_descriptor_da
    
     except Exception as e:
        print("Error occurred while loading descriptor CSV data:", e) 

# Returns the  first column (num_mixtures,1) of the concentration matrix   
def get_first_column ( concentrations_file_path):
    
    try:
           
        # Load data from the second CSV file as pandas dataframe
        df = pd.read_csv(concentrations_file_path, sep=',' ,encoding='latin-1' )   
        
        print("concentrations df content  ", df)
        
        # store mixture name column as pandas series
        first_column_mixture = df.iloc[:, 0]
        
        # convert pandas series to numpy array 
        first_column_mixture_ndarray = first_column_mixture.values
    
        num_mixture = df.shape[0]     
        print("mixture_name before adding first slot shape ", num_mixture)
 
        # reshape first_column_mixture_ndarray from (num_mixtures, ) to (num_mixtures, 1)    
        column_mixtures_reshape = np.reshape(first_column_mixture_ndarray, (num_mixture, 1))
        
        # Add an element of Component/Descriptor to the begining of the array
        column_mixtures_reshape = np.insert (column_mixtures_reshape.astype(str), 0, "Component/Descriptor" )    
          
        # Resahpe the mixture_name
        mixture_name = column_mixtures_reshape.reshape(1, -1) 
        print("mixture_name after adding first slot shape ", mixture_name.shape) # (19, )
        
        return mixture_name
    
    except Exception as e:
        print("Error occurred while loading concentration CSV data:", e)        


def get_num_mixtures(concentrations_file_path):
    
    try:       
        # Read the descriptors file using pandas to get the number of rows
        df = pd.read_csv(concentrations_file_path)         
        
        num_mixtutes = df.shape[0]
        
        print("num_mixtutes", num_mixtutes)
        print("num_mixtutes type " , type(num_mixtutes))   
        
        return num_mixtutes
    
    except Exception as e:
        print("Error occurred while reading concentrations CSV file:", e)
        return None
  
    
    
def get_num_components(concentrations_file_path):
     
    concentrations = load_concentrations_data(concentrations_file_path)
    df = pd.DataFrame(concentrations)
    
    # Use list comprehensions to extract nonzero elements for each column
    nonzero_concentrations = [[val for val in row if val != 0] for row in df.values]
    print ( "nonzero_concentrations  ", nonzero_concentrations)
    
    nonzero_concentrations = np.array(nonzero_concentrations, dtype=object)
    
    # Find the maximum length of arrays
    max_len = max(len(arr) for arr in nonzero_concentrations)
    print(  "num_components" , max_len) 
    print("num_components type " , type(max_len))                              
    
    return max_len


     
def get_num_descritors(descriptors_file_path):
    
    descriptors = load_descriptors_data(descriptors_file_path)
    num_descriptors = descriptors.shape[1]
    print("num_descriptors type " , type(num_descriptors))
    print("num_descriptors  " , num_descriptors)      
    return num_descriptors


        
def generate_combinatorial(descriptors_file_path, concentrations_file_path):    
    
   # load lazy descriptors to memory 
   descriptors =  generate_descriptors_based_nonzero_component (descriptors_file_path, concentrations_file_path )  
   
   # load lazy concentrations to memory
   concentrations = generate_nonzero_concentrations (concentrations_file_path)
   # print ("concentrations shape  ", concentrations.compute().shape)   
   
   num_components = get_num_components (concentrations_file_path)    
   num_mixtures = get_num_mixtures(concentrations_file_path)
   
   num_descriptors = get_num_descritors(descriptors_file_path)
   
   mix_descriptors = np.power (num_descriptors, num_components ) 
   
   # you can change the chunksize  from 1e5 to 1e8 depending on datasets output and resourses
   chunk_desc =   int (mix_descriptors / (num_components * num_components)  )
   chunk_comp = num_components
   product_chunk = (chunk_desc, chunk_comp) 
   
                           
   def cartesian_product(array):
       
       print("array of cartesian_product type ", type(array))
       array = array.astype(float)
       
       def product_mapper(x):
          
           prod = np.array(list(itertools.product(*x)))
           print("prod shape ", prod.shape) 
           
           return prod
       
       cartesian_product_dask = da.map_blocks(product_mapper, array, dtype= object, chunks= product_chunk)      
       print("cartesian_product_dask_func type ", type(cartesian_product_dask.shape))
       print ("cartesian_product_dask_func chunk:  ", cartesian_product_dask.chunks) 
       print("cartesian_product_dask_func elements  type: ", cartesian_product_dask.dtype) 
              
       return cartesian_product_dask
    
   chunk_mixtures = num_mixtures 
    
   combinatorial_chunk = (chunk_mixtures, 1 , chunk_desc )
   
   # Return the combinarorial mixture descriptors of two array
   def combinatorial_descriptor(x, y):
       
        print("x of combinatorial_descriptor type ", type(x))
        print("y of combinatorial_descriptor type ", type(y))
        
        print("x combinatorial shape:", len(x) )
        print("y combinatorial shape:", len(y))
        
                
        def combine_mapper(x, y):
            
            print("x of combine_mapper type ", type(x))
            print("y of combine_mapper type ", type(y))
            print("x of combine_mapper length ", len(x))
            print("y of combine_mapper shape ", y.shape)
            
            y = y.astype(float)
 
            result_list = []

            for i in range(num_mixtures):
                cartesian_subarray = x[i]
                concentration_row = y[i].reshape( num_components, 1 )
                result = np.dot( cartesian_subarray, concentration_row )
                result_list.append(result)
                
                
            print("result_list dot length ", len (result_list))
            final_result = np.array(result_list)
            print("final_result dot shape ", final_result.shape)  # ( 18, 27000000, 1 )
             
            
            final_result = np.transpose(final_result ,  (0, 2, 1) ) #  ( 18, 1,  27000000 )
            print("final_result dot shape ", final_result.shape)
            
           
            return final_result
        
        # Map combine_mapper function blockwise
        combinatorial_da = da.map_blocks( combine_mapper, x, y, chunks= combinatorial_chunk, dtype= float )
        
        # print("combinatorial_da_func shape ", combinatorial_da.compute().shape)
        print("combinatorial_da_func type ", type(combinatorial_da)) 
        print ("combinatorial_da_func chunk:  ", combinatorial_da.chunks) 
        print("combinatorial_da_func elements  type: ", combinatorial_da.dtype)                            
        
        return combinatorial_da
        
                
                                                    
   # Call the cartesian product function 
   cartesian_descriptors = [cartesian_product(subarray) for subarray in descriptors]
   print("cartesian_descriptors type ", type(cartesian_descriptors))  # <class 'list'>
   print ("cartesian_descriptors shape ", len(cartesian_descriptors))
   

  
   print(f"Cluster Dashboard URL: {cluster.dashboard_link}")
   print("Cluster Status:")
   print(client)
       
   client.cluster.scale(20)
       
   print(f"Cluster Dashboard URL: {cluster.dashboard_link}")
   print("Cluster Status:")
   print(client)
   
   sleep(5)
   
   
   
   # Scatter the large cartesian product list array
   cartesian_future = client.scatter(cartesian_descriptors)
   print("cartesian_future type ", type(cartesian_future)) 
     
   
   # Submit the concentration dask array to cluster 
   concentrations_future = client.submit (lambda x : x , concentrations)
   print("concentrations_future type ", type(concentrations_future))
   
   
    # Call combinatorial_descriptor (dot product) to cartesian_future and concentrations_future distributedely 
   combinatorial_future = client.submit( combinatorial_descriptor, cartesian_future, concentrations_future) 
   print("combinatorial_future type ", type(combinatorial_future))  
   
  
   result = client.gather(combinatorial_future)   
   print("result gather 1  type", type(result))   
   print ("result gather 1  shape", result.shape)  # (18, 1, 333333)
   
   
   result = result.compute()
   print ("result   compute type: ", type(result))  
   print ("result  compute: ", result.shape)
   
   
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
             print ("prod shape", prod.shape)
             return prod
          
          cartesian_product_dask = da.map_blocks(product_mapper, array, dtype = object, chunks= product_chunk)
          print ("cartesian_product_dask  shape: ", cartesian_product_dask.compute().shape)
          print ("cartesian_product_dask chunk:  ", cartesian_product_dask.chunks) 
          print("cartesian_product_dask  type: ", type(cartesian_product_dask)) 
          print("cartesian_product_dask elements  type: ", cartesian_product_dask.dtype)
          
          return cartesian_product_dask
            
      
      # Define a function to convert each element of the Cartesian product into a single comma-separated string
      def join_elements(cartesians):
          
          def join_mapper(x):
               # join = np.apply_along_axis(lambda row: list[','.join(map(str, row))], axis = 1, arr= x)
               join = np.array (list(map('_'.join, x)))
               print ("join shape: ", join.shape) 
               join_reshaped = join [:, np.newaxis]
               print ("join_reshaped shape: ", join_reshaped.shape) 
               return join_reshaped
          
          out_chunk = (row_chunk, 1)
          
          cartesian_strings = da.map_blocks (join_mapper, cartesians, dtype = object, chunks=  out_chunk)
          print ("cartesian_strings chunk:  ", cartesian_strings.chunks) 
          print("cartesian_strings  type: ", type(cartesian_strings)) 
          print("cartesian_strings elements  type: ", cartesian_strings.dtype)  
          # print ("cartesian_strings  shape: ", cartesian_strings.compute().shape)
          
          return cartesian_strings
          
      # Call the Cartesian product function  
      cartesian_header_descriptors = cartesian_product (header_descriptor)     
      
      # Scatter the large cartesian product array
      cartesian_header_future = client.scatter(cartesian_header_descriptors)     
      # print ("cartesian_header_descriptors shape: ", cartesian_header_future.result().shape ) 
       
      #  Call join_elements  to  cartesian_header_future   distributedely 
      cartesian_header_strings_future = client.submit (join_elements, cartesian_header_future)
      print ("cartesian_header_strings type: ", type(cartesian_header_strings_future))          
      
      # Convert the future object to a local Dask arrays
      cartesian_header= client.gather(cartesian_header_strings_future)
      print("cartesian_header content ", cartesian_header) 
      print ("cartesian_header type: ", type(cartesian_header))      
      print ("cartesian_header shape: ", cartesian_header.shape)  
      print("cartesian_header chunks: ", cartesian_header.chunks)
      

      cartesian_header = da.transpose (cartesian_header)
      print ("cartesian_header.T type: ", type(cartesian_header))  
      print ("cartesian_header.T shape: ", cartesian_header.shape)
       
      cartesian_header = cartesian_header.compute()
      print ("cartesian_header 2 type: ", type(cartesian_header))  
      print ("cartesian_header 2 shape: ", cartesian_header.shape)     
               
      return cartesian_header
 
    
    
# Function gets the dask array table and output path and write dask array to csv and returns file path dictionaty    
def write_to_csv(table_arr, output_path):
    
    print ("table_arr  type: ", type(table_arr))   
    print ("table_arr shape : ", table_arr.shape) 
    
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
       print("CSV file written successfully.")
       print ("CSV file size is  " , os.path.getsize(file_path))
       print ("CSV file column number is  " , table_df.shape[1])
       print ("file_path_dictionary is  " , file_path_dict)
       return file_path 
   
    except Exception as e:
       print("Error occurred while writing matrices to CSV:", e)
       
       
# Function gets the descriptors and concentrations and output the result of mixture descriptors concatenated with the header mixture name and first column mixture descriptors names 
def get_result(descriptors_file_path,concentrations_file_path, output_path ,threshold_const, threshold_corr, batch_num):
    
    num_mixtures = get_num_mixtures(concentrations_file_path)
     
    descriptor_name = combinatorial_header(descriptors_file_path , concentrations_file_path  )
    print("descriptor_name  type:", type (descriptor_name))         
    print("descriptor_name shape ", descriptor_name.shape)     
    
    mixture_names = get_first_column(concentrations_file_path).astype('object') 
    print("mixture_names  type:", type (mixture_names))      
    print("mixture_names  shape:",  mixture_names.shape)
    
    # transpose mixture_names to (-1, 1) 
    mixture_names = np.transpose (mixture_names) 
    print("mixture_names  type:", type (mixture_names))
    print("mixture_name content", mixture_names)    
    
    
    result = generate_combinatorial(descriptors_file_path, concentrations_file_path).astype(np.dtype('float32') ) 
    print("result 1  type:", type (result))
    print("result 1 shape ", result.shape)  
    print("result 1 computed element type:", result.dtype) 
    
    
    result = result.reshape( num_mixtures, -1 )
    print("result 2 shape ", result.shape)  
    # print("Result 2 chunks: ", result.chunks)
    
    result = result.astype('object')
    print("result 3 shape ", result.shape)  
    print("result 3 computed element type:", result.dtype)  


    concatenated = np.vstack((descriptor_name, result)) 
    
    print("concatenated  type:", type (concatenated))
    print("concatenated shape", concatenated.shape)
    
    concatenated_arr = np.hstack ((mixture_names, concatenated))
    
    print("concatenated_arr  type:", type (concatenated_arr))
    print("concatenated_arr shape", concatenated_arr.shape)         
    
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
    print("column_variance_future type ", type(column_variance_future))
    print("column_variance_future ", column_variance_future)  
    
    column_variance = client.gather(column_variance_future)
    print("column_variance  gather shape ", column_variance.shape) 
    print("column_variance gather type  ", type(column_variance)  )
    
    column_variance = column_variance.compute() 
    print("column_variance  computed  shape ", column_variance.shape) 
    print("column_variance computed type  ", type(column_variance)  )
    
    column_to_keep = column_variance > threshold
    print("column_to_keep variance  shape ", column_to_keep.shape) 
    print("column_to_keep variance  type  ", type(column_to_keep))
    print("column_to_keep variance  elements type  ", column_to_keep.dtype)
    
    column_to_keep = np.insert(column_to_keep, 0, True)
    print("column_to_keep  shape ", column_to_keep.shape) 
    print("column_to_keep type  ", type(column_to_keep))
    
    filtered_constants_arr = combinatorial_descriptors_arr[: , column_to_keep]
    print("filtered_constants_arr shape ", filtered_constants_arr.shape) 
    print("filtered_constants_arr type  ", type(filtered_constants_arr)  )
    print ("filtered_constants_arr column number is " , filtered_constants_arr.shape[1])
    
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
    print("combinatorial_descriptors 1 type ", type(combinatorial_descriptors))  
    print("combinatorial_descriptors 1 shape ", combinatorial_descriptors.shape)
    
    num_mixture = combinatorial_descriptors.shape[0]
    print("num_mixture  ", num_mixture)
    mix_descriptors = combinatorial_descriptors.shape[1]
    print("mix_descriptors  ", mix_descriptors)
   
    columns_to_keep_result = []
    
    batch_size = int ( mix_descriptors / batch_num )

    # Process the matrix in batches
    for start in range(0, mix_descriptors, batch_size):
        end = min(start + batch_size, mix_descriptors)
        
        # Extract the batch of columns
        batch = combinatorial_descriptors [:, start: end]  
        
        keep_col = highly_correlated_columns (batch, threshold_corr)
        print("keep_col  type ", type(keep_col)  )
        print("keep_col set  length  ", len(keep_col) )
        
        keep_col = np.array(list(keep_col))
        print("keep_col  type ", type(keep_col)  )
        print("keep_col  elements type ", keep_col.dtype  )
        keep_col += start
        columns_to_keep_result.append ( keep_col.tolist() )
        print("columns_to_keep_result  type ", type(columns_to_keep_result)  )
        print("columns_to_keep_result  length  ", len(columns_to_keep_result)  )
        
    
    if len(columns_to_keep_result) > 0:        
        
        columns_to_keep = np.concatenate(columns_to_keep_result) 
        print("columns_to_keep  type ", type(columns_to_keep)  )
        print("columns_to_keep elements type ", columns_to_keep.dtype)
        print("columns_to_keep  length ", len(columns_to_keep))
        print("columns_to_keep  shape ", columns_to_keep.shape)
        
        
        columns_to_keep += 1
        print("columns_to_keep type ", type(columns_to_keep)) 
        print("columns_to_keep shape ", len(columns_to_keep))  
        print("columns_to_keep elements  type: ", columns_to_keep.dtype) 
        
        matrix_high_corr = combinatorial_descriptors_arr [:, columns_to_keep ] 

    else: 
        
        columns_to_keep = np.array([], dtype = bool)
        print("columns_to_keep is empty") 
        matrix_high_corr = combinatorial_descriptors_arr 

     
    return matrix_high_corr

