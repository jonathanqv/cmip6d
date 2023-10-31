import os, shutil, wget, csv, time, datetime,cftime
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
                if row.text not in self.variables:
                    url_end = '/'.join([URL_BASE,row.text])
                    url_end=url_end.replace('catalog','ncss/grid')
                    year = url_end.split('_')[-1].split('.')[0]
                    extra = F"?var={self.varvar}&north={self.ymax}&west={self.xmin}&east={self.xmax}&south={self.ymin}&disableProjSubset=on&horizStride=1"
                    extra_time = F"&time_start={year}-01-01T12%3A00%3A00Z&time_end={year}-12-31T12%3A00%3A00Z&timeStride=1&addLatLon=true"
                    urls_lv_list.append(url_end + extra + extra_time)
            else:
                if row['href'].startswith(self.URL_END):
                    pass
                else:
                    urls_lv_list.append(['/'.join([URL_BASE,row['href']]),row.text])
        return urls_lv_list
    def get_links(self,out_path,check_links=False):
        """
        This creates the models folders and a csv file with the link
        """
        # URLS
        #.______________________
        URL_BASE = "https://ds.nccs.nasa.gov/thredds/catalog/AMES/NEX/GDDP-CMIP6/" # "https://ds.nccs.nasa.gov/thredds2/catalog/AMES/NEX/GDDP-CMIP6/"
        self.URL_END = "catalog.html"
        # Model level
        #.______________________
        level1= self.url_lv(URL_BASE+self.URL_END,URL_BASE)
        for url_1,name_1 in level1:
            # print(url_1,name_1) #!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!
            token = False
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
                token = True
                continue
            for url_2,name_2 in level2:
                # print('\t',url_2,name_2) #!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!
                if token == True:
                    break
                name_2_var = name_2.split('/')[0]
                if name_2_var in self.ssp:
        # r1i1p1f1 level
        #.______________________
                    # print('\t\t','quesito')
                    # print('\t\t',url_2,URL_BASE+name_1+name_2)
                    # print('inputlevel3',url_2,'/'.join([URL_BASE,name_1,name_2]))
                    level3=self.url_lv(url_2,'/'.join([URL_BASE,name_1,name_2]))
                    for url_3,name_3 in level3:
                        # print('\t\t',url_3,name_3) #!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!
                        if token == True:
                            break
        # Variable level
        #.______________________
                        level4=self.url_lv(url_3,'/'.join([URL_BASE,name_1,name_2,name_3]))  
                        # Checking if variables are present
                        total_level4 = [i.split('/')[0] for i in pd.DataFrame(level4)[1].values]
                        if False in [i in total_level4 for i in self.variables]:
                            print(F"Skipped {name_1} - {name_2} - variables not found")
                            token = True
                            break
                        for url_4,name_4 in level4:
                            # print('\t\t\t','level 4')
                            # print('\t\t\t',url_4,name_4) #!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!

                            varvar=name_4.split('/')[0]
                            if varvar in self.variables:
                                self.varvar = varvar
        # Final level, getting links
        #.______________________
                                level5=self.url_lv(url_4,'/'.join([URL_BASE,name_1,name_2,name_3,name_4]),final=True)
                                # print(level5) #!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!
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
                                    joker = work[0]
                                    try: # To check if reformatting to 12-30 is needed
                                        self.work_log(joker)
                                    except:
                                        joker = joker.replace("-12-31T12%", "-12-30T12%") 
                                        wget.download(joker,self.var_path)
                                        # if successfull
                                        work = [i.replace("-12-31T12%", "-12-30T12%")  for i in work]
                                        pd.DataFrame(work).to_csv(link_path,header=None,index=None,sep=' ')
                                except:
                                    tok_break = True
                                    shutil.rmtree(mod_path)
                                    print(F"Removed model {m}")
                                    continue
                                p = Pool(nworker)
                                p.map(self.work_log, work[1:])
    def work_log(self,work_data): # Created separate function as it was producing an error
        wget.download(work_data,self.var_path)
    def merge_files(self,cont=False):
        for m in os.listdir(self.out_path):
            if len(self.models) == 0:
                pass
            else:
                if m not in self.models:
                    continue
                else:
                    pass
            mod_path = os.path.join(self.out_path,m)
            for ssp in os.listdir(mod_path):
                if ssp in self.ssp:
                    ssp_path = os.path.join(mod_path,ssp)
                    for var in os.listdir(ssp_path):
                        if var in self.variables:
                            outf = os.path.join(ssp_path,F"{var}.nc")
                            # If file exists
                            if cont:
                                if os.path.isfile(outf):
                                    print(F"\nFile already exists {m} {ssp} {var}")
                                    continue
                            print(F"\nProcessing {m} {ssp} {var}")
                            var_path = os.path.join(ssp_path,var)
                            nc = [os.path.join(var_path,i) for i in os.listdir(var_path) if i.endswith('.nc')]
                            var_nc = xr.open_mfdataset(nc)    
                            var_nc.to_netcdf(outf)
    def get_csv(self,cont=False,historic=False):
        # self.todel = []
        self.todel = {}
        if historic:
            date_range = pd.date_range('1950-01-01 12','2014-12-31 12')
        else:
            date_range = pd.date_range('2015-01-01 12','2100-12-31 12')
        # Model level
        #.__________________________
        for m in os.listdir(self.out_path):
            token = False
            if len(self.models) == 0:
                pass
            else:
                if m not in self.models:
                    continue
                else:
                    pass
            # if m in nu:
            #     continue
            mod_path = os.path.join(self.out_path,m)
        # ssp level
        #.__________________________
            for ssp in os.listdir(mod_path):
                if token:
                    break
                if ssp in self.ssp:
                    ssp_path = os.path.join(mod_path,ssp)
                    nc = [os.path.join(ssp_path,i) for i in os.listdir(ssp_path) if i.endswith('.nc')]
                    for variable in nc:
                        if token:
                            break
                        var = variable.split('/')[-1].split('.nc')[0]
        # Check if dataframe exist
        #.__________________________
                        coord_df_path = os.path.join(ssp_path,F'coordinates{var}.csv')
                        data_df_path = os.path.join(ssp_path,F'{var}.csv')
                        if cont:
                            if (os.path.isfile(coord_df_path)) & (os.path.isfile(data_df_path)):
                                print(F"\n Already exists {m} {ssp} {var}")
                                continue
        # Opening netcdf
        #.________________                                
                        var_ds = xr.open_dataset(variable)
        # Checking times
        #.________________
                        time_arr = var_ds.indexes['time'].tolist()
                        time_sel = []
                        # Checking if it's datetime64
                        if isinstance(time_arr[0],np.datetime64):
                            time_arr = [pd.Timestamp(i) for i in time_arr.values]
                        if isinstance(time_arr[0],cftime._cftime.DatetimeNoLeap):
                            time_arr = var_ds.indexes['time'].to_datetimeindex().tolist()
                        # Checking with date_range
                        if len(set(date_range) - set(time_arr))>1:
                            # self.todel.append(m)
                            self.todel[m] = set(date_range) - set(time_arr)
                            token = True
                            break
        # Getting variables
        #.________________
                        print(F"\n Processing {m} {ssp} {var}")
                        lat = var_ds.lat.values.flatten()
                        lon = var_ds.lon.values.flatten()
                        lon = np.array([i-360 if i>180 else i for i in lon]).flatten() # Correcting
        # Creating csv files
        #.________________
                        dataDict = {'Date':time_arr}
                        coordinates = {}
                        for j in range(len(lat)):
                            for i in range(len(lon)):
                                values = var_ds[var].values[:,j,i]
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
        #             break
            #     break
            # break
        return self.todel                                              