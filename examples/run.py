# run.py
from  combinatorixPy import get_result
from dask.distributed import Client, LocalCluster, Worker


if __name__ == '__main__':  
     
    cluster = LocalCluster( n_workers= 3,  threads_per_worker= 128, memory_limit='500GB',  timeout= 3000)
    
    #Create the Client using the cluster
    client = Client(cluster)  
   
    descriptors_file_path = r'Descriptors.csv'
    concentrations_file_path = r'Components.csv'
    output_path = r'output'
    
    # Define thresholds for column variation, correlation, and standard deviation
    threshold_const = 0.01
    threshold_corr = 0.95
    batch_num = 1000
    
    result_path = get_result(descriptors_file_path, concentrations_file_path, output_path, threshold_const, threshold_corr, batch_num )
    print  ("result path", result_path) 
      
   
    scheduler_address = client.scheduler.address
    print("Scheduler Address:", scheduler_address)
    print(cluster.workers)
    print(cluster.scheduler)
    print(cluster.dashboard_link)

    print(cluster.status)   
    
    
    logs = cluster.get_logs()
    print('logs' , logs)
    
    # Get address of node 1 scheduler
    addr = cluster.scheduler_address
    print('Address' , addr)
    
    # On each additional node
    worker = Worker(addr)
    
    client.close()
    cluster.close()
