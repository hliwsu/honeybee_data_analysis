# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 19:20:24 2020

@author: haoli
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
from datetime import datetime
import datetime as dt
from varname import nameof
import simplejson
font = {'family' : 'Arial',
        'weight' : 'normal',
        'size'   : 20}
 
matplotlib.rc('font', **font)

starting_clock = datetime.now()
print(starting_clock)
var_na = ["Date", "Colony Size", "Adult Drones", "Adult Workers", "Foragers", "Active Foragers", "Drone Brood", "Worker Brood",             "Drone Larvae", "Worker Larvae", "Drone Eggs", "Worker Eggs", "Total Eggs", "DD", "L", "N", "P", "dd",                "l", "n", "Free Mites", "Drone Brood Mites", "Worker Brood Mites", "Mites/Drone Cell", "Mites/Worker Cell", "Mites ying", "Proportion Mites Dying",                "Colony Pollen (g)", "Pollen Pesticide Concentration", "Colony Nectar", "Nectar Pesticide Concentration",                "Dead Drone Larvae", "Dead Worker Larvae", "Dead Drone Adults", "Dead Worker Adults", "Dead Foragers",                "Queen Strength", "Average Temperature (celsius)", "Rain", "Min Temp", "Max Temp", "Daylight hours", "Forage Inc", "Forage Day",                "New Worker Eggs", "New Drone Eggs", "Worker Eggs To Larvae", "Drone Eggs To Larvae", "Worker Larvae To Brood", "Drone Larvae To Brood", "Worker Brood To Adult", "Drone Brood To Adult",                 "Drone Adults Dying", "Foragers Killed By Pesticides", "Worker Adult To Foragers", "Winter Mortality Foragers Loss", "Foragers Dying"]
future_model_list=["BNU-ESM","CCSM4","CNRM-CM5","CSIRO-Mk3-6-0","CanESM2","GFDL-ESM2G","GFDL-ESM2M","HadGEM2-CC365","HadGEM2-ES365","IPSL-CM5A-LR","IPSL-CM5A-MR","IPSL-CM5B-LR","MIROC-ESM-CHEM","MIROC5","MRI-CGCM3","NorESM1-M","bcc-csm1-1-m","bcc-csm1-1","inmcm4"]
cold_storage_period_list=["09-15_02-15","09-15_02-22","09-15_02-29","09-15_03-01","09-15_03-08","09-15_03-15","09-22_02-15","09-22_02-22","09-22_02-29","09-22_03-01","09-22_03-08","09-22_03-15","09-29_02-15","09-29_02-22","09-29_02-29","09-29_03-01","09-29_03-08","09-29_03-15","10-06_02-15","10-06_02-22","10-06_02-29","10-06_03-01","10-06_03-08","10-06_03-15","10-13_02-15","10-13_02-22","10-13_02-29","10-13_03-01","10-13_03-08","10-13_03-15","10-20_02-15","10-20_02-22","10-20_02-29","10-20_03-01","10-20_03-08","10-20_03-15","no cold storage"]
county_list=["Omak",'Richland','Walla Walla','Wenatchee']
rcp_list=["rcp45","rcp85"]
endash='_cold_storage_'
suffix='.txt'
fixed_hist_path='/data/rajagopalan/Honeybee/data/historical/cold-storage-simulations-observed/'
fixed_future_path='/data/rajagopalan/Honeybee/data/future/cold-storage-results/'
year = [i for i in range(1979,2100)]

### Functions ###
# cnty: type in county directly
# csd_h: type in cold storage periods
def hist_path(cnty, csd_h):
    if csd_h=="no cold storage":
        hist_fl_path=fixed_hist_path+cnty+'/observed/historical_default.txt'
    else:
        hist_fl_path=fixed_hist_path+cnty+'/observed/historical_cold_storage_'+csd_h+suffix
    return(hist_fl_path)

# xys=hist_path('Omak', "09-22_03-01")

def ftr_path(rcp, ftr_mdl, cnty, csd_f):
    if csd_f=="no cold storage":
        ftr_fl_path=fixed_future_path+cnty+'/'+rcp+'/'+ftr_mdl+'_default.txt'
    else:
        ftr_fl_path=fixed_future_path+cnty+'/'+rcp+'/'+ftr_mdl+endash+csd_f+suffix
    return(ftr_fl_path)
# =============================================================================
# Data reading module
# =============================================================================
def data_reading(path,var_list):
    data = pd.read_csv(path, delim_whitespace=True, header=None, names=var_na, skiprows=6)
    data_df=data[var_list]
    data_df=data_df.iloc[1:]
    data_df.rename(columns={ data_df.columns[0]: "dates" }, inplace = True)
    return(data_df)  

def ymd_index(df):
    df['dates']=pd.to_datetime(df['dates'], format='%m/%d/%Y')
    df=df.set_index(df['dates'])
    del df['dates']
    return(df)

# h_t=hist_path('Omak', "09-22_03-01")
# h_c=hist_path('Omak', "no cold storage")
# f_t=ftr_path('rcp85', "BNU-ESM", "Omak", "09-22_03-01")
# f_c=ftr_path('rcp85', "BNU-ESM", "Omak", "no cold storage")

def data_pointer(loc, cs_duration, rcp, model):
    h_t=hist_path(loc, cs_duration)
    h_c=hist_path(loc, "no cold storage")
    f_t=ftr_path(rcp, model,loc, cs_duration)
    f_c=ftr_path(rcp, model,loc, "no cold storage")
    return(h_t,h_c,f_t,f_c)

# h_t, h_c, f_t, f_c=data_pointer('Omak',"09-22_03-01",'rcp85',"BNU-ESM")
# h_t_df=ymd_index(data_reading(h_t, var_list))
# h_c_df=ymd_index(data_reading(h_c, var_list))
# f_t_df=ymd_index(data_reading(f_t, var_list))['2017':]
# f_c_df=ymd_index(data_reading(f_c, var_list))['2017':]

def data_stack_all_model(h_t_df,h_c_df,f_t_df,f_c_df,location, model):
    temp0=pd.concat([h_t_df, f_t_df], axis=0).add_suffix("_treated_"+model+'_'+location)
    temp1=pd.concat([h_c_df, f_c_df], axis=0).add_suffix("_untreated_"+model+'_'+location)
    temp=pd.concat([temp0, temp1], axis=1)
    return(temp)
# df_bnu_omak=data_stack_all_model(h_t_df,h_c_df,f_t_df,f_c_df,"BNU-ESM", "Omak")
def overall(var_list, model, location, rcp, cs_duration):
    h_t=hist_path(location, cs_duration)
    h_c=hist_path(location, "no cold storage")
    f_t=ftr_path(rcp, model, location, cs_duration)
    f_c=ftr_path(rcp, model, location, "no cold storage")
    h_t_df=ymd_index(data_reading(h_t, var_list))
    h_c_df=ymd_index(data_reading(h_c, var_list))
    f_t_df=ymd_index(data_reading(f_t, var_list))['2017':]
    f_c_df=ymd_index(data_reading(f_c, var_list))['2017':]
    temp=data_stack_all_model(h_t_df,h_c_df,f_t_df,f_c_df,location, model)
    return(temp)

def annual_min(df):
    tam=pd.DataFrame(df.groupby(df.index.year).agg('min')).reset_index()
    tam['dates']=pd.to_datetime(tam['dates'], format='%Y')
    tam=tam.set_index(tam['dates'])
    del tam['dates']
    return(tam)
def nth_day(df):
    s=df.reset_index()
    ask=[]
    for i in range(len(s['dates'])):
        ss=s['dates'][i]
        tt=ss.timetuple()
        a=tt.tm_yday
        ask.append(a)
    df["nth day of the year"]=ask
    return(df)

def month_sel(df):
    df=df.loc[df.index.month==(9 or 10 or 11)]
    return(df)


data_collection={'Year':year }
data_collection_df=pd.DataFrame(data_collection)
var_list=['Date','Drone Brood', 'Drone Larvae' ]

# application# 

# =============================================================================
# omk_bnu_85_915215=month_sel(nth_day(overall(var_list, "BNU-ESM", 'Omak', rcp_list[1], "09-15_02-15")))
# omk_bnu_85_915215_min_larva_t=omk_bnu_85_915215.groupby(omk_bnu_85_915215.index.year).apply(lambda x: x.sort_values('Drone Larvae_treated_BNU-ESM_Omak', ascending=True).head(1))
# s1_t_val=omk_bnu_85_915215_min_larva_t['Drone Larvae_treated_BNU-ESM_Omak'].tolist()
# s1_t_date=omk_bnu_85_915215_min_larva_t['nth day of the year'].tolist()
# 
# omk_bnu_85_915215_min_larva_u=omk_bnu_85_915215.groupby(omk_bnu_85_915215.index.year).apply(lambda x: x.sort_values('Drone Larvae_untreated_BNU-ESM_Omak', ascending=True).head(1))
# s1_u_val=omk_bnu_85_915215_min_larva_t['Drone Larvae_treated_BNU-ESM_Omak'].tolist()
# s1_u_date=omk_bnu_85_915215_min_larva_t['nth day of the year'].tolist()
# =============================================================================


for mdl in future_model_list:
    for loc in county_list:
        for rcp in rcp_list:
            for cold_dur in cold_storage_period_list:
                dataset_temp=month_sel(nth_day(overall(var_list, mdl, loc, rcp, cold_dur)))
                # process brood (treated)
                dataset_temp_t=dataset_temp.groupby(dataset_temp.index.year).apply(lambda x: x.sort_values('Drone Brood_treated_'+mdl+'_'+loc, ascending=False).head(1))
                brood_t_val=dataset_temp_t['Drone Brood_treated_'+mdl+'_'+loc].tolist()
                brood_t_date=dataset_temp_t['nth day of the year'].tolist()
                data_collection_df[mdl + '_' +loc + '_' + rcp+ '_' +cold_dur+'_max value']=brood_t_val
                data_collection_df[mdl + '_' +loc + '_' + rcp+ '_' +cold_dur+'_nth day of the year']=brood_t_date
                # process brood (untreated)
                dataset_temp_u=dataset_temp.groupby(dataset_temp.index.year).apply(lambda x: x.sort_values('Drone Brood_untreated_'+mdl+'_'+loc, ascending=False).head(1))
                brood_u_val=dataset_temp_u['Drone Brood_untreated_'+mdl+'_'+loc].tolist()
                brood_u_date=dataset_temp_u['nth day of the year'].tolist()
                data_collection_df[mdl + '_' +loc + '_' + rcp+ '_' +cold_dur+'_untreated_max value']=brood_u_val
                data_collection_df[mdl + '_' +loc + '_' + rcp+ '_' +cold_dur+'_untreated_nth day of the year']=brood_u_date                
                # process larvae (treated)
                dataset_temp_t=dataset_temp.groupby(dataset_temp.index.year).apply(lambda x: x.sort_values('Drone Larvae_treated_'+mdl+'_'+loc, ascending=True).head(1))                
                larvae_t_val=dataset_temp_t['Drone Larvae_treated_'+mdl+'_'+loc].tolist()
                larvae_t_date=dataset_temp_t['nth day of the year'].tolist()
                data_collection_df[mdl + '_' +loc + '_' + rcp+ '_' +cold_dur+'_untreated_max value']=larvae_t_val
                data_collection_df[mdl + '_' +loc + '_' + rcp+ '_' +cold_dur+'_untreated_nth day of the year']=larvae_t_date
                 # process larvae (untreated)
                dataset_temp_u=dataset_temp.groupby(dataset_temp.index.year).apply(lambda x: x.sort_values('Drone Larvae_untreated_'+mdl+'_'+loc, ascending=True).head(1))                
                larvae_u_val=dataset_temp_u['Drone Larvae_untreated_'+mdl+'_'+loc].tolist()
                larvae_u_date=dataset_temp_u['nth day of the year'].tolist()
                data_collection_df[mdl + '_' +loc + '_' + rcp+ '_' +cold_dur+'_untreated_max value']=larvae_u_val
                data_collection_df[mdl + '_' +loc + '_' + rcp+ '_' +cold_dur+'_untreated_nth day of the year']=larvae_u_date               
                print(mdl + '_' +loc + '_' + rcp+ '_' +cold_dur)

data_collection_df.to_csv('aa')                
ending_clock = datetime.now()
print(ending_clock-starting_clock)                  
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
