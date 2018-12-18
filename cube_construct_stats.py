# -*- coding: utf-8 -*-
"""
Created on Fri Jun 15 13:28:31 2018

@author: rthomas
"""
#import calendar
import datetime
#from erddapy import ERDDAP
import numpy as np
import os
import pandas as pd

# Set working directory for files to be saved
working = os.path.normpath('C:/Users/rthomas/test_area/dashboard')

# Choose from 'wave_spectral', 'wave_zero' or 'weather'
data_types = ['wave_spectral', 'wave_zero', 'weather']

source = 'url' # Populate with one of ['file','erddapy','url']

for typed in data_types:
    # Name files to save data
    datafile = os.path.join(working,'%s/%s_data_raw.csv' % (typed, typed))
    availfile = os.path.join(working,'%s/%s_data_availability.csv' % (typed, typed))
    dailyfile = os.path.join(working,'%s/%s_daily_summary.csv' % (typed, typed))
    
    # Set global variables without 'buoy_id', 'latitude' and 'longitude'
    now = datetime.date.today()
    metadata = ['station_id',
                'time']
    print(typed)
    # Set variables based on data type (typed)
    if typed.lower() == 'wave_spectral':
        dset_id = 'IWaveBNetwork_spectral'
        syear = 2008    
        master_params = ['PeakPeriod',
                         'PeakDirection',
                         'PeakSpread',
                         'SignificantWaveHeight',
                         'EnergyPeriod',
                         'MeanWavePeriod_Tm01',
                         'MeanWavePeriod_Tm02',
                         'qcflag']
        
    elif typed.lower() == 'wave_zero':
        dset_id = 'IWaveBNetwork_zerocrossing'
        syear = 2008    
        master_params = ['Hmax',
                         'HmaxPeriod',
                         'Havg',
                         'Tavg',
                         'qcflag']
    
    elif typed.lower() == 'weather':
        dset_id = 'IWBNetwork'
        syear = 2001
        master_params = ['AtmosphericPressure',
                         'WindDirection',
                         'WindSpeed',
                         'Gust',
                         'WaveHeight',
                         'WavePeriod',
                         'MeanWaveDirection',
                         'Hmax',
                         'AirTemperature',
                         'DewPoint',
                         'SeaTemperature',
                         'RelativeHumidity',
                         'QC_Flag']
    
    # Get data from selected source
    if source.lower() not in ['file','erddapy','url']:
        print("Please check option has been entered correctly.")
    else:
        if source.lower() == 'erddapy': # Use ERDDAP toolbox to call data from MI ERDDAP
            e = ERDDAP(server='https://erddap.marine.ie/erddap',
                       protocol='tabledap',)
            e.response = 'csv'
            e.dataset_id = dset_id
            e.constraints = {
                'time>=': '%s-01-01T00:00:00Z' % (syear),
                'time<=': '%sT00:00:00Z' % (now.strftime('%Y-%m-%d')),
            }
            e.variables = metadata + master_params        
            df = e.to_pandas()
            
            df.to_csv(datafile, sep=',') # Save data to file
            
        elif source.lower() == 'url': # Manually generated ERDDAP URL call
            df = pd.DataFrame()
            # Generate parameter component of URL
            plist = ''
            for item in metadata + master_params:
                plist = plist+item+'%2C'
            plist = plist[0:-3]    
            # Iterate by year to reduce risk of time out
            years = range(syear,now.year)
            for year in years:    
                url = "https://erddap.marine.ie/erddap/tabledap/"+dset_id+".csv?"+plist+"&time%3E="+str(year)+"-01-01T00:00:00Z&time%3C"+str(year+1)+"-01-01T00:00:00Z"
                dfbyyear = pd.read_csv(url,index_col=1,header=[0],skiprows=[1],parse_dates=True,infer_datetime_format=True)
                df = pd.concat([df,dfbyyear])
                print("Downloaded %s" % (year))
            # Final call for data from start of this year upto and including yesterday
            url = "https://erddap.marine.ie/erddap/tabledap/"+dset_id+".csv?"+plist+"&time%3E="+str(now.year)+"-01-01T00:00:00Z&time%3C"+now.strftime('%Y-%m-%d')+"T00:00:00Z"
            dfbyyear = pd.read_csv(url,index_col=1,header=[0],skiprows=[1],parse_dates=True,infer_datetime_format=True)
            df = pd.concat([df,dfbyyear])
            print("Downloaded %s" % (str(now.year)))
            df.to_csv(datafile, sep=',') # Save data to file
            print("Raw data downloaded and saved to csv.")
    
        elif source.lower() == 'file': # Load from file if working offline and data has previously been downloaded from ERDDAP
            df = pd.read_csv(datafile, index_col=0, parse_dates=True, infer_datetime_format=True)
            
        else:
            print("Error in code logic. Please check.")
    
    # Utilise quality control flags to clean data set
    # Code to be written
    
    # Get data coverage by day, month, year, Day of Year and Date for each variable
    # Add columns for date variables
    #df['DayofYear'] = df.index.dayofyear
    df['Date'] = df.index.date
      
    df_summ = df.groupby(['station_id','Date']).count().reset_index(level=['station_id','Date'])
    
    df_avail = pd.DataFrame()  
    for stn in df_summ.station_id.unique().tolist():
        df_stn = df_summ[df_summ['station_id']==stn]
        if typed == 'weather' or stn == 'Westwave MK4':
            res=24
        else:
            res=48
        df_stn.loc[:,master_params] = df_stn.loc[:,master_params]/res*100
        df_fulldates = pd.DataFrame(index = pd.date_range(df_stn.Date.min() - datetime.timedelta(days=df_stn.Date.min().day-1), df_stn.Date.max()))
        df_fulldates['Date'] = df_fulldates.index.date
        df_fulldates = df_fulldates.merge(df_stn, how='outer', left_on='Date', right_on='Date').fillna(0)
        df_fulldates.station_id = stn
        
        df_avail = pd.concat([df_avail,df_fulldates])
    
    df_avail = df_avail.set_index(['station_id', 'Date'])
    if typed != 'weather':
        df_avail = df_avail.drop(['qcflag'], axis=1)
    else:
        df_avail = df_avail.drop(['QC_Flag'], axis=1)
    df_avail.columns = pd.MultiIndex.from_product([df_avail.columns, ['avail']])

       
    # Save availability data to file
    df_avail.to_csv(availfile, sep=',')
    print("Data availability saved to file.")

    #%% Make daily summary statistics
    # Split out parameter types for different summary statistics
    
    params = []
    param_dir = []
    for item in master_params:
        if 'qc' not in item.lower():
            if 'Dir' in item:
                param_dir.append(item)
            else:
                params.append(item)
    #%% Take a copy of the data
    data = df
            
    # Get north and east components for directional measurements
    param_comp = []
    for dirtn in param_dir:
        data['%s_n' % (dirtn)] = np.cos(data[dirtn]*np.pi/180)
        param_comp.append('%s_n' % (dirtn))
        data['%s_e' % (dirtn)] = np.sin(data[dirtn]*np.pi/180)
        param_comp.append('%s_e' % (dirtn))
        
    # Resample for summary statistics for non-directional measurements
    daily = data.groupby(['station_id','Date'])[params].agg(['min','max','mean','std'])
    
    if len(param_dir)!=0:
        # Resample for mean and std for directional measurement components (north and east)
        data2 = data.groupby(['station_id','Date'])[param_comp].agg(['mean','std'])
           
        # Recalculate direction mean and std from averaged components (north and east)
        # Add directly into daily dataframe
        for dirtn in param_dir:
            daily[(dirtn, 'mean')] = (360 + np.arctan2(data2[('%s_e' % (dirtn), 'mean')], data2[('%s_n' % (dirtn), 'mean')]) * 180/np.pi) % 360
            daily[(dirtn, 'std')] = (360 + np.arctan2(data2[('%s_e' % (dirtn), 'std')], data2[('%s_n' % (dirtn), 'std')]) * 180/np.pi) % 360
            daily[(dirtn, 'max')] = np.nan
            daily[(dirtn, 'min')] = np.nan
   
    # Sort daily dataframe
    daily = daily[sorted(daily.columns.tolist())]
    
    # Save statistical data to file
    daily.to_csv(dailyfile, sep=',')
    print("Daily summary data saved to csv.\n")