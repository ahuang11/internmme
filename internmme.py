import matplotlib
matplotlib.use('agg')

import os
import time
import glob
import param
import parambokeh
import subprocess
import numpy as np
import xarray as xr
import pandas as pd
import geoviews as gv
import holoviews as hv
import geoviews.feature as gf

from holoext.xbokeh import Mod
from holoext.utils import get_cmap
from holoviews.streams import Stream
from holoviews.operation.datashader import regrid

hv.extension('bokeh')

# INCREMENT THE MONTH WITH NEW DATA
INIT_DT = pd.datetime(2018, 5, 8)  # must be 8
VARIABLES = ['tmp2m', 'prate']
VIEW_VARIABLE = VARIABLES[0]  # read only tmp2m
MEAN_FI = '{dt:%Y%m}.nc'.format(dt=INIT_DT)

# defaults; no need to change
MODELS = [
    'CFSv2',
    'CMC1',
    'CMC2',
    'GFDL',
    'GFDL_FLOR',
    'NCAR_CCSM4',
    'NASA_GEOS5v2'
]

WGET_FMT = (
    'wget -nc '  # nc -> no clutter
    'http://ftp.cpc.ncep.noaa.gov/'
    'NMME/realtime_anom/'
    '{model}/{dt:%Y%m%d%H}/'
    '{model}.{variable}.{dt:%Y%m}.anom.nc'
)
DATA_DIR = 'data'

def multithread_dl():
    os.chdir(DATA_DIR)

    if not os.path.exists(MEAN_FI):
        dl_cmds = [WGET_FMT.format(model=model, dt=INIT_DT, variable=variable)
                   for model in MODELS for variable in VARIABLES]

        for cmd in dl_cmds:
            dl_fi = cmd.split('/')[-1]
            if not os.path.exists(dl_fi):
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
                time.sleep(5)  # sleep 5 second to prevent overload
        
        try:
            process.communicate()
        except:
            print('All files exists in {0}!'.format(DATA_DIR))
    else:
        print('Mean file exists; skipping download')

    os.chdir('..')
    return

def build_ds(variable):
    os.chdir(DATA_DIR)

    if not os.path.exists(MEAN_FI):
        glob_fmt = '*{variable}.{dt:%Y%m}*.nc'.format(
            variable=variable, data_dir=DATA_DIR, dt=INIT_DT)

        ds_list = []
        for ncfi in glob.glob(glob_fmt):
            ds = xr.open_dataset(ncfi, decode_times=False)

            model_var_tup = ncfi.split('.')[0:2]
            ds.coords['model'] = model_var_tup[0]
            ds.coords['variable'] = model_var_tup[1]
            ds_list.append(ds)

        ds = xr.concat(ds_list, 'model')
        ds_mean = ds.mean('ensmem')
        ds_mean.to_netcdf(MEAN_FI)
    else:
        print('Mean is cached!')
        ds_mean = xr.open_dataset(MEAN_FI, decode_times=False)

    since = pd.to_datetime(ds_mean.target.units, format='months since %Y-%m-%d 00:00:00')
    ds_mean.target.values = [(since + pd.offsets.MonthBegin(month)).strftime('%Y-%m-%d')
                             for month in ds_mean.target.values]
    ds_mean.target.attrs['long_name'] = 'Target'
    ds_mean.target.attrs['units'] = 'YYYY-MM-DD'

    os.chdir('..')

    return ds_mean

def load_models(models, target):
    gvds_sel = gvds.select(model=models, target=target)
    gvds_sel = gvds_sel.aggregate(['lon', 'lat'], np.mean)
    return Mod(
        logo=False,
        width=950,
        height=750,
        tools=['hover'],
        xticks=[],
        yticks=[],
        xlabel=' ',
        ylabel=' ',
        title='NMME {0}'.format(VIEW_VARIABLE),
        colorbar_cmap='RdBu_r'
    ).apply(gv.Image(gvds_sel) * gf.coastline())


multithread_dl()
ds_mean = build_ds(VIEW_VARIABLE)
targets = ds_mean.target.values

gvds = gv.Dataset(ds_mean).redim.range(fcst=(-4, 4))

class ModelSelector(Stream):
    target = param.ObjectSelector(
        default=targets[0],
        objects=targets,
        precedence=0.1
    )
    models = param.ListSelector(
        default=MODELS,
        objects=MODELS
    )
    output = parambokeh.view.Plot()
    
    def view(self, *args, **kwargs):
        return load_models(self.models, self.target)

    def event(self, **kwargs):
        self.output = hv.DynamicMap(self.view, streams=[self])

selector = ModelSelector(name='InterNMME')
parambokeh.Widgets(selector,
                   callback=selector.event,
                   continuous_update=False,
                   view_position='right',
                   on_init=True,
                   mode='server')