
import datetime

import datatable as dt
import numpy as np
import pandas as pd
import os
import sys
sys.path.append('TableauOpt')
from TableauHyper import buildTabDefination, HyperException, csv2hyper


if __name__=="__main__":

    time1 = datetime.datetime.now()
    print("Create df %s" % time1)
    data_dir = 'test_data'
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)    

    hyper_path = 'test_data/test.hyper'
    csv_path = 'test_data/test_for_hyper.csv'
    
    sample_df_nrows = 1000
    df = pd.DataFrame(
        dict(
            id=range(sample_df_nrows), 
            value=np.random.randn(sample_df_nrows),
            name='xxxxxxxxxxxxxxxxxxxx'
            )
        )

    sample_df_ncols = 27
    for i in range(sample_df_ncols):
        df['col_%d' % i] = df['name']

    print(df.shape)
    df_dt = dt.Frame(df)
    time2 = datetime.datetime.now()
    print("Write csv start %s" % time2)
    df_dt.to_csv(csv_path)
    
    time3 = datetime.datetime.now() 
    print("Build hyper file start %s" % time3)
    try:
        table_def = buildTabDefination(df, 'RandomValues')
        csv2hyper(table_def, csv_path, hyper_path)
    except HyperException as ex:
        print(ex)
        
    time_end = datetime.datetime.now()
    print("Ends: %s" % (time_end))
    print("create df costs: %s" % (time2 - time1))
    print("Write csv cost: %s" % (time3 - time2))
    print("hyper file Costs: %s" % (time_end - time3))
