
from progressBar import progressbar
from collections import defaultdict
import readConfig as rCfg
import pandas as pd
import subprocess
import argparse
import datetime
import psutil
import shutil
import json
import sys
import os
import re

defaultArgs = {
    'byYear':True,
    'byMonth':True,
    'parseDate':True,
    'standardizeNames':True,
    'overWrite':False,
    'reset':False,
    'fileType':'',
    'nameTag':'',
    'searchTag':[],
    'excludeTag':[],
    'timeShift':'',
    'mode':'copy',
}

def set_high_priority():
    p = psutil.Process(os.getpid())
    p.nice(psutil.HIGH_PRIORITY_CLASS)

class mapEcDir():
    def __init__(self,dIn = None,dOut=None,**kwargs):
        self.dIn = dIn
        self.dOut = dOut
        # Apply defaults where not defined
        kwargs = defaultArgs | kwargs
        # add arguments as class attributes
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.config = rCfg.set_user_configuration(tasks={'fileTypes':'config_files/ecFileFormats.yml'})
        if self.dIn is not None:
            if self.fileType !='':
                fileInfo=self.config['fileTypes'][self.fileType]
            else: 
                fileInfo = None
            if self.dOut is None:
                inventory = self.dIn+'/fileInventory.csv'
            else:
                if os.path.isdir(self.dOut) == False:
                    os.makedirs(self.dOut)
                inventory = self.dOut+'/fileInventory.csv'
            if self.reset == True:    
                uIn = input('Warning: type "reset" to confirm the reset\n')
                if uIn == 'reset':
                    if self.dOut is not None and self.dOut != self.dIn:
                        shutil.rmtree(self.dOut)
                else:
                    sys.exit()
            elif os.path.isfile(inventory):
                self.fileInventory = pd.read_csv(inventory)
            self.buildInventory(fileInfo)
            if hasattr(self,'fileInventory'):
                self.fileInventory.to_csv(inventory,index=False)

    def buildInventory(self,fileInfo):
        print('Searching ',self.dIn)
        for dir, _, fileList in os.walk(self.dIn):
            if hasattr(self,'fileInventory'):
                fileList = [s for s in fileList if s not in self.fileInventory['source'].values]
            if self.searchTag !='':
                fileList = [s for s in fileList if sum(t in s for t in self.searchTag) == len(self.searchTag)]
            if self.excludeTag !='':
                fileList = [s for s in fileList if sum(t in s for t in self.excludeTag) == 0]
            if fileInfo is not None:
                fileList = [f for f in fileList if f.endswith(fileInfo['extension'])]
            source = [os.path.abspath(dir+'/'+f) for f in fileList]
            filename,Interval = self.reName(fileInfo,fileList)
            if len(source)>0:
                if self.dOut is None:
                    dpath = source
                else:
                    if self.byMonth == True and self.dOut != '':
                        dpath = [f"{self.dOut}/{p.year}/{str(p.month).zfill(2)}/{f}" for p,f in zip(Interval.to_period('M'),filename)]
                        for p in Interval.to_period('M').unique():
                            if os.path.isdir(f"{self.dOut}/{p.year}/{str(p.month).zfill(2)}")==False:
                                os.makedirs(f"{self.dOut}/{p.year}/{str(p.month).zfill(2)}")
                    elif self.byYear == True and self.dOut != '':
                        dpath = [f"{self.dOut}/{p.year}/{f}" for p,f in zip(Interval.to_period('Y'),filename)]
                        for p in Interval.to_period('Y').unique():
                            if os.path.isdir(f"{self.dOut}/{p.year}")==False:
                                os.makedirs(f"{self.dOut}/{p.year}")
                    else:
                        dpath = [self.dOut for f in source]
                    if os.path.isdir(self.dOut) == False:
                        os.makedirs(self.dOut)
                    pb = progressbar(len(source),f'{self.mode}: {dir.replace(self.dIn,"")}')
                    dpath = [os.path.abspath(d) for d in dpath]
                    for s,p,f in zip(source,dpath,filename):
                        if hasattr(self,'fileInventory') == False:
                            self.pasteWithSubprocess(s,p,option=self.mode)
                        elif s not in self.fileInventory['source'] and p not in self.fileInventory['dpath']:
                            if self.overWrite == True or f not in self.fileInventory['filename']:
                                self.pasteWithSubprocess(s,p,option=self.mode)
                        pb.step()
                    pb.close()
                if hasattr(self,'fileInventory'):
                    self.fileInventory=pd.concat([self.fileInventory,
                        pd.DataFrame(data={'Interval':Interval,
                                            'filename':filename,
                                            'dpath':dpath,
                                            'source':source
                        })],axis=0)
                else:
                    self.fileInventory = pd.DataFrame(data={'Interval':Interval,
                                                            'filename':filename,
                                                            'dpath':dpath,
                                                            'source':source})

    def reName(self,fileInfo,fileList):
        if self.parseDate == False: 
            filename = fileList
            Interval = [i for i in range(len(fileList))]
        else:
            srch = [re.search(fileInfo['search'], f.rsplit('.',1)[0]).group(0) for f in fileList]
            Interval = [datetime.datetime.strptime(s,fileInfo['format']) for s in srch]
            if self.timeShift != '':
                Interval = [t + pd.Timedelta(self.timeShift) for t in Interval]
            timeString = [datetime.datetime.strftime(t,fileInfo['format']) for t in Interval]
            if self.nameTag != '':
                self.searchTag.insert(0,self.nameTag)
            tag = '_'.join(self.searchTag)
            if len(tag)>0:tag=tag+'_'
            filename = [f"{tag}{ts}.{self.fileType}" for ts in timeString]
            Interval = pd.to_datetime(Interval)
            print(filename)
        return (filename,Interval)

    def pasteWithSubprocess(self,source, dest, option = 'copy',Verbose=False):
        set_high_priority()
        cmd=None
        source = os.path.abspath(source)
        dest = os.path.abspath(dest)
        if sys.platform.startswith("darwin"): 
            # These need to be tested/flushed out
            if option == 'copy' or option == 'xcopy':
                cmd=['cp', source, dest]
            elif option == 'move':
                cmd=['mv',source,dest]
        elif sys.platform.startswith("win"): 
            cmd=[option, source, dest]
            if option == 'xcopy':
                cmd.append('/s')
        if cmd:
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        if Verbose==True:
            print(proc)

if __name__ == '__main__':
    # Parse the arguments
    CLI=argparse.ArgumentParser()
    CLI.add_argument(f"--dIn",nargs="?",type=str,default=None)
    CLI.add_argument(f"--dOut",nargs="?",type=str,default=None)
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
    mapEcDir(**kwargs)
    
    