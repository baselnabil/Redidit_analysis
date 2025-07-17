import os



def check_file(dir= '/opt/airflow/data' , extension = '.done'):
    try:
        if not os.path.isdir(dir):
            raise ValueError(f'{dir} is not a directory')
        return any(
            f.endswith(extension) 
            for f in os.listdir(dir)
            if os.path.isfile(os.path.join(dir,f))
        )
    except Exception as e :
        print(e)
        return False
    

