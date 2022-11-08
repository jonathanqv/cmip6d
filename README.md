# cmip6d
[![DOI](https://zenodo.org/badge/563466510.svg)](https://zenodo.org/badge/latestdoi/563466510)

This python library downloads downscaled climate change scenarios from NASA NEX-GDDP-CMIP6 (https://ds.nccs.nasa.gov/thredds/catalog/AMES/NEX/GDDP-CMIP6/catalog.html).

## Instructions
To use this library install the requirements:

* wget
* csv
* numpy
* pandas
* xarray
* beautifulsoup4

If you are using windows, wget needs to be downloaded and added to your paths
If you don't want to get the .csv summary you do not need xarray, but it is highly recommended for post-processing.

## Install

`pip install cmip6d`

## Examples

To import the library:

```python
from cmip6d import cmip6d
```
Define the main variables. Until 
```python
out_path = 'test' # Defines your output folder
coords = [-75,-69.5,-17.5,-14] # xmin,xmax,ymin,ymax
models = [] # If empty, downloads everything, if not, downloads specified packages
ssp=['ssp245','ssp585'] # DEFAULT VARIABLE. Target scenarios from the NASA server
variables = ['pr','tasmax','tasmin'] # DEFAULT VARIABLE. Target variables from the NASA server
```
To create the main Python object:
```python
cc = cmip6d(out_path,coords,models=['ACCESS-CM2'],variables=variables,ssp=ssp)
```
First, it creates the folder structure based on the MODELS, then it generates a "link.txt" file with the links to be downloaded. The "check_links" argument allows you to not re-create the "link.txt" file if it already exists.
```python
cc.get_links(out_path,check_links=True)
```
To download the links you need to specify a number of workers "nworker", which speeds up the download. Once completed these step you will have all the netcdf files for your climate change model, these can be loaded with xarray or whatever other method you prefer.
```python
cc.download_links(nworker=4)
```
## Additional steps
If you would like to get 2 ".csv" files with coordinates of the following structure:

| ID    | Latitude | Longitude |
| ----  | -------- | --------- |
| P_0_0 |  ....    |  .....    |
| P_0_1 |  ....    |  .....    |
|  ...  |  ....    |  .....    |

and

| Date       | P_0_0 | P_0_1 | ... |
| ---------  | ----- | ----- |---- |
| 2015-01-01 |  .... | ..... | ... |
| 2015-01-02 |  .... | ..... | ... |
| ...        |  .... | ..... | ... |

You can use the following function after running the previous ones, where "cont=True" does not process the data if the files already exist, and "nu" represent the models you would like not to process.
```python
cc.get_csv(self,cont=True,nu=['CESM2','CESM2-WACCM','IITM-ESM','HadGEM3-GC31-MM'])
```