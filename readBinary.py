# Create a CSV file from the binary database 
# Written by June Skeeter

# Basic test-call from command line:
    # py csvFromBinary.py --siteID BBS --dateRange "2023-06-01 00:00" "2024-05-31 23:59"
# Call with user defined request file (s)
    # py csvFromBinary.py --siteID BBS --dateRange "2023-06-01 00:00" "2024-05-31 23:59" --requests C:/path_to/request1.yml C:/path_to/request2.yml
# Can also call from other python scripts, using this general syntax:
    # import csvFromBinary as cfb
    # cfb.makeCSV(siteID="BBS",dateRange=["2023-06-01 00:00","2024-05-31 23:59"],requests=["config_files/csv_requests_template.yml"])
# Setup the config files for your environment accordingly before running

import os
import sys
import yaml
import json
import argparse
import numpy as np
import pandas as pd
from datetime import datetime,date

# Import the collocated dependencies regardless of wd
wd = os.getcwd()
cd = os.path.split(__file__)[0]
sys.path.insert(1,cd)
print(os.getcwd())
import readConfig
sys.path.remove(cd)
os.chdir(wd)

template = 'config_files/user_defined/read_binary_template.yml'
defaultDateRange = [date(datetime.now().year,1,1),datetime.now()]

# Default arguments
defaultArgs = {
    'wd':os.path.dirname(os.path.realpath(__file__)),
    'dateRange':[date(datetime.now().year,1,1).strftime("%Y-%m-%d"),datetime.now().strftime("%Y-%m-%d")],
    'database':'None',
    'outputPath':'None',
    'requests':[template,template],
    'stage':'Clean/SecondStage',
    'nameTimeStamp':True,
    'saveDf':False,
    'debug':False
    }

class fromDatabase():
    def __init__(self,siteID,**kwargs):
        self.siteID = siteID
        abspath = os.path.abspath(__file__)
        self.dname = os.path.dirname(abspath)
        # Set to cwd to location of the current script
        os.chdir(self.dname)
        if isinstance(kwargs, dict):
            pass
        elif os.path.isdir(kwargs):
            with open(kwargs) as yml:
                kwargs = yaml.safe_load(yml)
        else:
            sys.exit(f"Provide a valid set of arguments, either from a yaml file or a dict")

        # Apply defaults where not defined
        kwargs = defaultArgs | kwargs
        # add arguments as class attributes
        for k, v in kwargs.items():
            setattr(self, k, v)
            
        if type(self.requests)!= dict:
            self.config = readConfig.set_user_configuration(requests={'requests':self.requests})
        else:
            self.config = readConfig.set_user_configuration()
            if 'stage' not in self.requests.keys():
                self.config['requests'] = self.requests
            else:
                self.config['requests'] = {'dfOut':self.requests}
                
        # Root directory of the database
        if self.database == 'None':
            self.rootDB = self.config['rootDir']['Database']
        else: 
            self.rootDB = kwargs['database']
        
        # Years to process
        self.dateRange = pd.DatetimeIndex(kwargs['dateRange'])
        self.Years = range(self.dateRange.year.min(),self.dateRange.year.max()+1)
        self.results = {}
        for name,task in self.config['requests'].items():
            self.results[name] = self.request(task)
            if self.saveDf == True:
                self.save(name)
        if list(self.results.keys())==['dfOut']:
            self.results = self.results['dfOut']
        if self.debug == False:
            del self.df

    
    def request(self,task):
        if 'stage' not in task.keys():
            task['stage'] = self.stage
        if 'traces' not in task.keys():
            sys.exit('Improper request format')
        # Create a list of column header - unit tuples
        # Only used if units_in_header set to True
        # Create a blank dataframe
        df = pd.DataFrame()
        DT = self.readTime(task)
        traces = self.readTraces(task)
        
        # dump traces to dataframe
        df = pd.DataFrame(data=traces,index=DT)
        # limit to requested timeframe
        self.df = df.loc[((df.index>=self.dateRange.min())&(df.index<= self.dateRange.max()))].copy()
        if 'formatting' in task.keys():
            self.formatDF(task['formatting'],task['traces'])
        return(self.df)

    def readTime(self,task):
        # Read time vector, prefers posix timestamp, but will default to matlab dateum if not available
        try:
            file = f"{self.siteID}/{task['stage']}/{'POSIX_timestamp'}"
            tv = np.concatenate([np.fromfile(f"{self.rootDB}/{YYYY}/{file}",self.config['dbase_metadata']['POSIX_timestamp']['dtype']) for YYYY in self.Years],axis=0)
            DT = pd.to_datetime(tv,unit=self.config['dbase_metadata']['POSIX_timestamp']['base_unit']).round('s')
        except:
            print('No POSIX timestamp available, defaulting to MATLAB datenum')
            file = f"{self.siteID}/{task['stage']}/{'clean_tv'}"
            tv = np.concatenate([np.fromfile(f"{self.rootDB}/{YYYY}/{file}",self.config['dbase_metadata']['clean_tv']['dtype']) for YYYY in self.Years],axis=0)
            DT = pd.to_datetime(tv-self.config['dbase_metadata']['clean_tv']['base'],unit=self.config['dbase_metadata']['clean_tv']['base_unit']).round('s')
            pass
        # number of expected observations for each trace
        self.nExpected = tv.shape
        return(DT)

    def readTraces(self,task):
        # Create a dict of traces
        traces={}
        if type(task['traces']) == dict:
            traceList = task['traces'].keys()
        else:
            traceList = task['traces']
        # Loop through race list for request
        for trace_name in task['traces']:
            # if exists (over full period) output
            try:
                file = f"{self.siteID}/{task['stage']}/{trace_name}"
                traces[trace_name] = np.concatenate([np.fromfile(f"{self.rootDB}/{YYYY}/{file}",self.config['dbase_metadata']['traces']['dtype']) for YYYY in self.Years],axis=0)
                if traces[trace_name].shape != self.nExpected:
                    trip = ('-1')**.5
            # give NaN if traces does not exist or has incorrect number of data points
            except:
                print(f"{trace_name} missing or corrupted, outputting NaNs")
                traces[trace_name]=np.empty(self.nExpected )*np.nan
        return(traces)

    def formatDF(self,format,traces):
        if type(traces) == dict:
            header = [t['output_name'] if type(t) == dict and 'output_name' in t.keys() else k for k,t in traces.items()]
            if 'units_in_header' in format.keys() and format['units_in_header'] == True:
                U = [t['units'] if type(t) == dict and 'units' in t.keys() else '' for t in traces.values()]
                header = [(h,u) for h,u in zip(header,U)]
                self.df.columns = pd.MultiIndex.from_tuples(header)
        if 'time_vectors' in format.keys():
            for time_trace,timeFormat in format['time_vectors'].items():
                if len(self.df.columns[0])>1:
                    if 'units' in timeFormat.keys() and 'output_name' in timeFormat.keys():
                        time_trace = (timeFormat['output_name'],timeFormat['units'])
                    elif 'output_name' in timeFormat.keys():
                        time_trace = (timeFormat['output_name'],'')
                    else: 
                        time_trace = (time_trace,timeFormat['units'])
                elif 'output_name' in timeFormat.keys():
                    time_trace = timeFormat['output_name']
                self.df[time_trace] = self.df.index.floor('Min').strftime(timeFormat['fmt'])
        
        if 'na_value' in format.keys() and format['na_value'] is not None:
            self.df = self.df.fillna(format['na_value']).copy()

    def save(self,name):
        if self.outputPath != 'None' and os.path.isdir(self.outputPath):
            # Format filename and save output
            dates = self.dateRange.strftime('%Y%m%d%H%M')
            if self.nameTimeStamp == True:
                fn = f"{self.siteID}_{name}_{dates[0]}_{dates[1]}"
            else:
                fn = f"{self.siteID}_{name}"
            if os.path.isdir(self.outputPath) == False:
                os.makedirs(self.outputPath)
            dout = os.path.abspath(f"{self.outputPath}/{fn}.csv")
            self.df.to_csv(dout,index=False)
            self.results[name]=dout
            print(f'See output: {dout}')
        else:
            print('Give valid output path to save file')


# If called from command line ...
if __name__ == '__main__':
    
    CLI=argparse.ArgumentParser()
    
    CLI.add_argument(
        "--siteID", 
        nargs="?",# Use "?" to limit to one argument instead of list of arguments
        type=str,
        default=None,
        )    

    dictArgs = []
    for key,val in defaultArgs.items():
        dt = type(val)
        nargs = "?"
        if dt == type({}):
            dictArgs.append(key)
            dt = type('')
            val = '{}'
        elif dt == type([]):
            nargs = '+'
            dt = type('')
        CLI.add_argument(f"--{key}",nargs=nargs,type=dt,default=val)

    # parse the command line
    args = CLI.parse_args()
    kwargs = vars(args)
    for d in dictArgs:
        kwargs[d] = json.loads(kwargs[d])
    print(kwargs)
    fromDatabase(**kwargs)