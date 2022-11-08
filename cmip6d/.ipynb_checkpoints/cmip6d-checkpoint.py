import os, shutil, wget, csv, time, datetime
import xarray as xr
import numpy as np
import pandas as pd
import requests,pprint
from bs4 import BeautifulSoup
from multiprocessing import Pool

class cmip6d():
    def __init__(self,out_path,coords,models=[],variables=['pr','tasmax','tasmin'],ssp=['ssp245','ssp585']):
        """
        coords: list with xmin,xmax,ymin,ymax in lat long
        variables: Variables to download
        ssp: ssp scenarios to download
        models: if empty it downloads all of them, if not, it downloads the specified models
        """
        # Coordinates
        self.out_path = out_path
        self.xmin = coords[0]
        self.xmax = coords[1]
        self.ymin = coords[2]
        self.ymax = coords[3]
        if self.xmin<0:
            self.xmin = 360 + self.xmin
        if self.xmax<0:
            self.xmax = 360 + self.xmax
        # Variables
        self.variables=variables
        self.ssp = ssp
        self.models = models
    def url_lv(self,url,URL_BASE,final=False):
        # Scrapping
        #.______________________
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html5lib')
        table = soup.find('table')
        href = table.findAll('a')

        urls_lv_list = []
        for row in href:
            if final:
                if row.tt.text not in self.variables:
                    url_end = URL_BASE+row.tt.text
                    url_end=url_end.replace('catalog','ncss')
                    year = url_end.split('_')[-1].split('.')[0]
                    extra = F"?var={self.varvar}&north={self.ymax}&west={self.xmin}&east={self.xmax}&south={self.ymin}&disableProjSubset=on&horizStride=1&time_start={year}-01-01T12%3A00%3A00Z&time_end={year}-12-31T12%3A00%3A00Z&timeStride=1&addLatLon=true"
                    urls_lv_list.append(url_end + extra)
            else:
                if row['href'].startswith(self.URL_END):
                    pass
                else:
                    urls_lv_list.append([URL_BASE+row['href'],row.tt.text])
        return urls_lv_list
    def get_links(self,out_path,check_links=False):
        """
        This creates the models folders and a csv file with the link
        """
        # URLS
        #.______________________
        URL_BASE = "https://ds.nccs.nasa.gov/thredds2/catalog/AMES/NEX/GDDP-CMIP6/"
        self.URL_END = "catalog.html"
        # Model level
        #.______________________
        level1= self.url_lv(URL_BASE+self.URL_END,URL_BASE)
        for url_1,name_1 in level1:
            name_1_var = name_1.split('/')[0]
            if len(self.models) == 0:
                pass
            else:
                if name_1_var not in self.models:
                    continue
                else:
                    pass
            print(F"Starting {name_1}")
        # SSP level
        #.______________________
            level2=self.url_lv(url_1,URL_BASE+name_1)
            # checking if it has the models we are looking for
            total_level2 = [i.split('/')[0] for i in pd.DataFrame(level2)[1].values]
            
            if False in [i in total_level2 for i in self.ssp]:
                print(F"Skipped {name_1} - ssp not found")
                continue
            for url_2,name_2 in level2:
                name_2_var = name_2.split('/')[0]
                if name_2_var in self.ssp:
        # r1i1p1f1 level
        #.______________________
                    level3=self.url_lv(url_2,URL_BASE+name_1+name_2)
                    for url_3,name_3 in level3:
        # Variable level
        #.______________________
                        level4=self.url_lv(url_3,URL_BASE+name_1+name_2+name_3)
                        # Checking if variables are present
                        total_level4 = [i.split('/')[0] for i in pd.DataFrame(level4)[1].values]
                        if False in [i in total_level4 for i in self.variables]:
                            print(F"Skipped {name_1} - {name2} - variables not found")
                            continue
                        for url_4,name_4 in level4:
                            varvar=name_4.split('/')[0]
                            if varvar in self.variables:
                                self.varvar = varvar
        # Final level, getting links
        #.______________________
                                level5=self.url_lv(url_4,URL_BASE+name_1+name_2+name_3+name_4,final=True)
                                # Export paths
                                main_path = os.path.join(self.out_path,name_1,name_2,name_4)
                                # Checking if path exists
                                if os.path.isdir(main_path):
                                    pass
                                else:
                                    os.makedirs(main_path)
                                # Saving Urls
                                total_urls = []
                                for ffff in level5:
                                    total_urls.append(ffff)
                                # Check links?
                                links_path = os.path.join(main_path,'links.txt')
                                if check_links: # May need to check size of csv too, but looks not necessary
                                    if os.path.exists(links_path):
                                        print(F"Exists: {links_path}")
                                        continue
                                    else:
                                        pass
                                pd.DataFrame(total_urls).to_csv(links_path,header=None,index=None,sep=' ')
    def download_links(self,nworker=4):
        for m in os.listdir(self.out_path):
            tok_break = False
            # Checking if is  in mdoels list
            if len(self.models) == 0:
                pass
            else:
                if m not in self.models:
                    continue
                else:
                    pass
            mod_path = os.path.join(self.out_path,m)
            for ssp in os.listdir(mod_path):
                if tok_break == True:
                    break
                if ssp in self.ssp:
                    ssp_path = os.path.join(mod_path,ssp)
                    for var in os.listdir(ssp_path):
                        if tok_break == True:
                            break
                        if var in self.variables:
                            self.var_path = os.path.join(ssp_path,var)
                            link_path = os.path.join(ssp_path,var,'links.txt')
                            
                            if len(os.listdir(self.var_path)) == 87: # 86 files + 1 of links.txt
                                print(F"\n Completed {m} {ssp} {var}")
                                continue
                            elif len(os.listdir(self.var_path)) != 87:
                                [os.remove(os.path.join(self.var_path,i)) for i in os.listdir(self.var_path) if i!= 'links.txt']
                                print(F"\n Downloading {m} {ssp} {var}")
                                # Opening and downloading
                                links = pd.read_csv(link_path,header=None)
                                links.columns = ['link']
                                # Downloading in parallel
                                work = links.values.flatten().tolist()
                                try:
                                    self.work_log(work[0])
                                except:
                                    tok_break = True
                                    shutil.rmtree(mod_path)
                                    print(F"Removed model {m}")
                                    continue
                                p = Pool(nworker)
                                p.map(self.work_log, work[1:])
    def work_log(self,work_data): # Created separate function as it was producing an error
        wget.download(work_data,self.var_path)
    def get_csv(self,cont=False,nu=['CESM2','CESM2-WACCM','IITM-ESM','HadGEM3-GC31-MM']):
        date_range = pd.date_range('2015-01-01 12','2100-12-31 12')
        for m in os.listdir(self.out_path):
            # Checking if is  in mdoels list
            if len(self.models) == 0:
                pass
            else:
                if m not in self.models:
                    continue
                else:
                    pass
            # Filtering models without complete variables
            if m in nu:
                continue
            mod_path = os.path.join(self.out_path,m)
            for ssp in os.listdir(mod_path):
                if ssp in self.ssp:
                    ssp_path = os.path.join(mod_path,ssp)
                    for var in os.listdir(ssp_path):
                        if var in self.variables:
                            var_path = os.path.join(ssp_path,var)
            # Creating paths
            #.________________
                            coord_df_path = os.path.join(ssp_path,F'coordinates{var}.csv')
                            data_df_path = os.path.join(ssp_path,F'{var}.csv')
                            if cont:
                                if (os.path.isfile(coord_df_path)) & (os.path.isfile(data_df_path)):
                                    print(F"\n Already exists {m} {ssp} {var}")
                                    continue
            # Opening netcdf
            #.________________
                            print(F"\n Processing {m} {ssp} {var}")
                            nc = [os.path.join(var_path,i) for i in os.listdir(var_path) if i!='links.txt']
                            var_nc = xr.open_mfdataset(nc)
            # Getting variables
            #.________________
                            lat = var_nc.lat.values.flatten()#[::-1]
                            lon = var_nc.lon.values.flatten()
                            lon = np.array([i-360 if i>180 else i for i in lon]).flatten()
            # Processing times, there are some errors in the files
            #.________________
                            # if not isinstance(time,pd.DatetimeIndex):
                            #     time = time.to_datetimeindex()
                            time_arr = var_nc.indexes['time']
                            time = []
                            # Checking if it's datetime64
                            if isinstance(time_arr.values[0],np.datetime64):
                                time_arr = [pd.Timestamp(i) for i in time_arr.values]
                            # Processing to skip incorrect dates like February 30
                            for ti in time_arr:
                                try:
                                    dt = datetime.datetime(ti.year,ti.month,ti.day,ti.hour)
                                    time.append(True)
                                except:
                                    print(F"\tError in date {ti.year}-{ti.month}-{ti.day}")
                                    time.append(False)
            # Filtering based on time series
            #.________________
                            var_nc= var_nc.sel(time=time)
                            # Checking time series
                            time = var_nc.indexes['time'].values
                            for dr in date_range:
                                if dr not in time:
                                    print(F"Missing Date {dr.strftime('%m/%d/%Y')}")
            # Creating csv files
            #.________________
                            if isinstance(var_nc['time'].values[0],np.datetime64):
                                dataDict = {'Date':[datetime.datetime(t.year,t.month,t.day,t.hour) for t in pd.to_datetime(time)]}
                            else:
                                dataDict = {'Date':[datetime.datetime(t.year,t.month,t.day,t.hour) for t in time]}
                            coordinates = {}
                            for j in range(len(lat)):
                                for i in range(len(lon)):
                                    values = var_nc[var].values[:,j,i]
                                    if np.isnan(values[0]): # If point in the ocean?
                                        continue
                                    else:
                                        dataDict[F'P_{i}_{j}'] = values
                                        coordinates[F'P_{i}_{j}'] = [lon[i],lat[j]] #lon,lat
                            coord_df = pd.DataFrame(coordinates).T
                            coord_df.columns = ['Longitude','Latitude']
                            coord_df.index.name = 'Code'
                            data_df = pd.DataFrame(dataDict)
                            data_df = data_df.set_index('Date')
            # Exporting
            #.________________
                            data_df.to_csv(data_df_path)
                            coord_df.to_csv(coord_df_path)