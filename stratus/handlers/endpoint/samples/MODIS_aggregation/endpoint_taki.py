from stratus_endpoint.handler.base import TaskResult
from typing import List, Dict
from stratus_endpoint.handler.execution import Executable, ExecEndpoint
import xarray as xa
import abc
import source.baseline.baseline_series_v8_dask_supriya as series
import numpy as np
from datetime import date, datetime
from dateutil.rrule import rrule, DAILY, MONTHLY
import h5py
import timeit
import pandas as pd
from netCDF4 import Dataset
import pdb
import json
from collections import OrderedDict

import dask
from dask.distributed import as_completed
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from dask.distributed import wait

from  .laads_data_download import  sync


class XaOpsEndpoint(ExecEndpoint):
    """
        This class is used to implement the capabilities of the Endpoint.
    """

    @abc.abstractmethod
    def createExecutable(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs) -> Executable:
        """
            Factory method for Executable objects.
            Creates an Executable for each analytics operation

            Parameters:
            requestSpec (Dict):         Dict which defines the analytics operation
            inputs: (List[TaskResult]): Inputs from other operations in the workflow

            Returns:
            Executable: A Executable object which execute the operation.
            """
        return XaOpsExecutable(requestSpec, inputs, **kwargs)

    def init(self):
        """
            Used to startup analytic resources such as scheduler, worker nodes, database, etc.
            """
        return

    def shutdown(self, **kwargs):
        """
            Used to shut down and release resources that were alloacted in the init method.
            """
        return

    def capabilities(self, type: str, **kwargs) -> Dict:
        """
            Used to return metadata describing the capabilities of the Endpoint.
            The only required response is the definition the Endpoint Address (epa) for this Endpoint.
            The epa is used to route operation requests to the an Endpoint that can process them.

            Parameters:
                type (str):  Optionally allows the definition of various types of capabilty requests.

            Returns:
                  Dict:  metadata describing the capabilities of the Endpoint.
            """
        return dict(epas=["xop*"])


class XaOpsExecutable(Executable):
    """
        This class is used to implement a single operation.
    """

    def execute(self, **kwargs) -> TaskResult:
        """
            Executes the operation.
            Creates an Executable for each analytics operation
            The operation request is available as self.request.
            The operation inputs are available as self.inputs.

            Returns:
            TaskResult: The result of the operation.
            """
        print(f"Executing request {self.request}")

        # python3 baseline_series_v8.py data_path.csv 2008/01/01 2008/01/01 [-90,90,-180,180] [1,1] [5] 1 1 1 1 1 1 1 input_file.csv input_Jhist.csv
        
        # print("usage: python aggre_stats_mpi.py <Data Path> <Start Date> <End Date> \
        #                                         <Polygon boundaries> <Lat & Lon Grid Size > \
        #                                         <Sampling number larger than 0> \
        #                                         <1/0> <1/0> <1/0> \
        #                                         <1/0> <1/0> <1/0> \
        #                                         <1/0> <Variable Imput File> <JHist Variable Imput File>")

        inputSpec = self.request.get('input', [])
        
        
        spl_num = np.int(inputSpec['spl_num'][1:-1])
        poly=np.fromstring(inputSpec['poly'][1:-1], dtype=np.int, sep=',' )
        grid=np.fromstring(inputSpec['grid'][1:-1], dtype=np.float, sep=',' )
        
        # Define the statistics names for HDF5 output
        sts_name = ['Minimum','Maximum','Mean','Pixel_Counts', \
                    'Standard_Deviation','Histogram_Counts','Jhisto_vs_']

        # Pass system arguments to the function
        # sts_switch = np.array(sys.argv[7:14],dtype=np.int)
        sts_switch = np.fromstring(inputSpec['sts_switch'],dtype=np.int, sep=',')
        sts_switch = np.array((sts_switch == 1))
        #varlist = inputSpec['varlist']

        # Read the variable names from the variable name list
        #text_file = np.array(pd.read_csv(varlist, header=0, delim_whitespace=True)) #open(varlist, "r")
        text_file = np.array(pd.DataFrame.from_dict(json.loads(inputSpec['varlist']))) 
        varnames  = text_file[:,0] 

        if sts_switch[5] == True: 
            intervals_1d = text_file[:,1] # This is a string interval arrays
        else: 
            intervals_1d = [0]

        if sts_switch[6] == True:   
            # Read the joint histogram names from the variable name list
            #jvarlist = inputSpec['jvarlist']
            #text_file = np.array(pd.read_csv(jvarlist, header=0, delim_whitespace=True)) #open(varlist, "r")
            text_file = np.array(pd.DataFrame.from_dict(json.loads(inputSpec['jvarlist']))) #open(varlist, "r")
            histnames = text_file[:,1] 
            var_idx   = text_file[:,2] #This is the index of the input variable name which is used for 2D histogram
            intervals_2d = text_file[:,3]
        else:
            intervals_2d,var_idx = [0],[0]

        #-------------STEP 1: Set up the specific directory --------
        # data_path_file = np.array(pd.read_csv(inputSpec['data_path_file'], header=0, delim_whitespace=True))
        data_path_file = np.array(pd.DataFrame.from_dict(json.loads(inputSpec['data_path_file'])))   
        if data_path_file is not None:
            MYD06_dir    = data_path_file[0,0] #'/umbc/xfs1/cybertrn/common/Data/Satellite_Observations/MODIS/MYD06_L2/'
            MYD06_prefix = data_path_file[0,1] #'MYD06_L2.A'
            MYD03_dir    = data_path_file[1,0] #'/umbc/xfs1/cybertrn/common/Data/Satellite_Observations/MODIS/MYD03/'
            MYD03_prefix = data_path_file[1,1] #'MYD03.A'
        else:
            #autodownloading the MYD03 and MYD06 files from the website.
            start = time.time()

            M03_source_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MYD03/2008/001/"
            M06_source_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MYD06_L2/2008/001/"
            M03_localpath = "../new_data/MYD03"
            M06_localpath = "../new_data/MYD06_L2"

            token =  "F43033A6-B1DB-11EA-9C3C-E3E73909D347"

            if not os.path.exists(M03_localpath):
                os.makedirs(M03_localpath)

            if not os.path.exists(M06_localpath):
                os.makedirs(M06_localpath)

            sync(M03_source_url, M03_localpath, token)
            sync(M06_source_url, M06_localpath, token)
            
            print("\n\nCompleted download in " + str(time.time() - start) + " seconds")

            MYD03_dir = M03_localpath
            MYD03_prefix = "MYD03.A"
            MYD06_dir = M06_localpath 
            MYD06_prefix = "MYD06_L2.A"




        fileformat = 'hdf'

        # output_path_file = np.array(pd.read_csv(inputSpec['data_path_file'], header=3, delim_whitespace=True))
        output_path_file = np.array(pd.DataFrame.from_dict(json.loads(inputSpec['output_path']))) 
        output_dir = output_path_file[0,0]
        output_prefix = output_path_file[0,1]

        #-------------STEP 2: Set up spactial and temporal resolution & variable names----------
        NTA_lats = [poly[0],poly[1]] #[  0,40] #[-90,90]   #[-30,30]    
        NTA_lons = [poly[2],poly[3]] #[-40,60] #[-180,180] #[-60,60]  
        
        gap_x, gap_y = grid[1],grid[0] #0.5,0.625

        if ((NTA_lons[-1]-NTA_lons[0])%gap_x != 0) | ((NTA_lats[-1]-NTA_lats[0])%gap_y != 0): 
            print("Grid size should be dividable by the dimension of the selected region.")
            print("If you choose the region of latitude  from -40 to 40, then you gird size (Latitude ) should be dividable by 80.")
            print("If you choose the region of longitude from  20 to 35, then you gird size (Longitude) should be dividable by 55.")
            print("Please try again!")
            sys.exit()

        map_lon = np.arange(NTA_lons[0],NTA_lons[1],gap_x)
        map_lat = np.arange(NTA_lats[0],NTA_lats[1],gap_y)
        Lon,Lat = np.meshgrid(map_lon,map_lat)
        grid_lon=np.int((NTA_lons[-1]-NTA_lons[0])/gap_x)
        grid_lat=np.int((NTA_lats[-1]-NTA_lats[0])/gap_y)

        #--------------STEP 3: Create arrays for level-3 statistics data-------------------------
        t_grid_data = {}
        bin_num1 = np.zeros(len(varnames)).astype(np.int)
        bin_num2 = np.zeros(len(varnames)).astype(np.int)
        key_idx = 0
        for key in varnames:
            if sts_switch[0] == True:
                t_grid_data[key+'_'+sts_name[0]] = np.zeros(grid_lat*grid_lon) + np.inf
            if sts_switch[1] == True:
                t_grid_data[key+'_'+sts_name[1]] = np.zeros(grid_lat*grid_lon) - np.inf
            if (sts_switch[2] == True) | (sts_switch[3] == True) | (sts_switch[4] == True):
                t_grid_data[key+'_'+sts_name[2]] = np.zeros(grid_lat*grid_lon)
                t_grid_data[key+'_'+sts_name[3]] = np.zeros(grid_lat*grid_lon)
                t_grid_data[key+'_'+sts_name[4]] = np.zeros(grid_lat*grid_lon)
            if sts_switch[5] == True:
                bin_interval1 = np.fromstring(intervals_1d[key_idx], dtype=np.float, sep=',' )
                bin_num1[key_idx] = bin_interval1.shape[0]-1
                t_grid_data[key+'_'+sts_name[5]] = np.zeros((grid_lat*grid_lon,bin_num1[key_idx]))

                if sts_switch[6] == True:
                    bin_interval2 = np.fromstring(intervals_2d[key_idx], dtype=np.float, sep=',' )
                    bin_num2[key_idx] = bin_interval2.shape[0]-1
                    t_grid_data[key+'_'+sts_name[6]+histnames[key_idx]] = np.zeros((grid_lat*grid_lon,bin_num1[key_idx],bin_num2[key_idx]))

            key_idx += 1

        # Sort the dictionary by alphabetizing
        t_grid_data = OrderedDict(sorted(t_grid_data.items(), key=lambda x: x[0]))
        print("t_grid_data")
        print(t_grid_data)

        
        print("Output Variables for Level-3 File:")
        for key in t_grid_data:
            print(key)

        # Read the filename list for different time period
        fname1,fname2 = [],[]

        start_date = np.fromstring(inputSpec['start_date'], dtype=np.int, sep='/' )
        end_date   = np.fromstring(inputSpec['end_date'], dtype=np.int, sep='/' )
        start = date(start_date[0], start_date[1], start_date[2])
        until = date(end_date[0], end_date[1], end_date[2])

        for dt in rrule(DAILY, interval=1, dtstart=start, until=until):
            year  = np.int(dt.strftime("%Y"))
            month = np.int(dt.strftime("%m"))
            day   = np.int(dt.strftime("%d"))

            data = datetime(year,month,day)
            daynew = data.toordinal()
            yearstart = datetime(year,1,1)
            day_yearstart = yearstart.toordinal()
            day_in_year = (daynew-day_yearstart)+1

            yc ='%04i' % year
            dc ='%03i' % day_in_year

            fname_tmp1 = series.read_filelist(MYD06_dir,MYD06_prefix,yc,dc,fileformat)
            fname_tmp2 = series.read_filelist(MYD03_dir,MYD03_prefix,yc,dc,fileformat)
            fname1 = np.append(fname1,fname_tmp1)
            fname2 = np.append(fname2,fname_tmp2)
        print('***********year************')
        print(year,month)
        print('***********month************')

        filenum = np.arange(len(fname1))
        print(len(fname1))










        #--------------STEP 6: Start Aggregation------------------------------------------------

        print('***********calling operate************')

        xds = self.operate(fname1,fname2,NTA_lats,NTA_lons,grid_lon,grid_lat,gap_x,gap_y,filenum, \
                                    t_grid_data,sts_switch,varnames,intervals_1d,intervals_2d,var_idx, spl_num, \
                                    sts_name, histnames, bin_num1, bin_num2, year, month, map_lat, map_lon, \
                                    output_dir, output_prefix)


        #resultDataset = xa.DataArray(xds, name='test')
        return TaskResult(kwargs, [xds])



    def operate(self, fname1,fname2,NTA_lats,NTA_lons,grid_lon,grid_lat,gap_x,gap_y,filenum, \
                                t_grid_data,sts_switch,varnames,intervals_1d,intervals_2d,var_idx, \
                                spl_num, sts_name, histnames, bin_num1, bin_num2, year, month, map_lat, map_lon, \
                                output_dir, output_prefix):
        """
            Convenience method defined for this particular operation
        """
      
        request = self.request['operation'][0]['name'].split(':')[1]

        if request == "modisAggr":

            # Start counting operation time
            start_time = timeit.default_timer() 
            print('***********calling run_modis_aggre************')

            kwargv = {"NTA_lats": NTA_lats, "NTA_lons": NTA_lons, "grid_lon": grid_lon,"grid_lat": grid_lat, "gap_x": gap_x, "gap_y": gap_y,\
             "filenum": filenum, "sts_switch":sts_switch, "varnames": varnames, "intervals_1d":intervals_1d, "intervals_2d":intervals_2d, \
             "var_idx":var_idx, "spl_num":spl_num, "sts_name":sts_name, "histnames":histnames}

            cluster = SLURMCluster(cores=1, memory='64 GB', project='pi_jianwu',\
                queue='high_mem', walltime='16:00:00', job_extra=['--exclusive', '--qos=medium+'])
            cluster.scale(4)
            client = Client(cluster)
            tt = client.map(series.run_modis_aggre, fname1, fname2, **kwargv)
            for future, result in as_completed(tt, with_results= True):
                print("future result")
                print(result)
                longname_list = result[1]
                result = result[0]
                # aggregate the result
                print("grid_lat*grid_lon:")
                print(grid_lat*grid_lon)
                for z in np.arange(grid_lat*grid_lon):
                    # For all variables
                    key_idx = 0
                    for key in varnames:
                        if sts_switch[0] == True:
                            if  t_grid_data[key+'_'+sts_name[0]][z] > result[key+'_'+sts_name[0]][z]:
                                t_grid_data[key+'_'+sts_name[0]][z] = result[key+'_'+sts_name[0]][z]
                        if sts_switch[1] == True:
                            if  t_grid_data[key+'_'+sts_name[1]][z] < result[key+'_'+sts_name[1]][z]:
                                t_grid_data[key+'_'+sts_name[1]][z] = result[key+'_'+sts_name[1]][z]
                        #Total and Count for Mean
                        if (sts_switch[2] == True) | (sts_switch[3] == True):
                            t_grid_data[key+'_'+sts_name[2]][z] += result[key+'_'+sts_name[2]][z]
                            t_grid_data[key+'_'+sts_name[3]][z] += result[key+'_'+sts_name[3]][z]
                        #standard deviation
                        if sts_switch[4] == True:
                            t_grid_data[key+'_'+sts_name[4]][z] += result[key+'_'+sts_name[4]][z]
                        #1D Histogram
                        if sts_switch[5] == True:
                            t_grid_data[key+'_'+sts_name[5]][z] += result[key+'_'+sts_name[5]][z]
                        #2D Histogram
                        if sts_switch[6] == True:
                            t_grid_data[key+'_'+sts_name[6]+histnames[key_idx]][z] += result[key+'_'+sts_name[6]+histnames[key_idx]][z]
                        key_idx += 1



            # Compute the mean cloud fraction & Statistics (Include Min & Max & Standard deviation)

            # Reference for statstic parameters
            # sts_name[0]: min
            # sts_name[1]: max
            # sts_name[2]: mean / total_value
            # sts_name[3]: count
            # sts_name[4]: square
            # sts_name[5]: histogram
            # sts_name[6]: joint histogram

            sts_idx = np.array(np.where(sts_switch == True))[0]
            print("Index of User-defined Statistics:",sts_idx)
            key_idx = 0
            for key in varnames:
                for i in sts_idx:
                    if i == 0:
                        t_grid_data[key+'_'+sts_name[0]] = t_grid_data[key+'_'+sts_name[0]].reshape([grid_lat,grid_lon])
                    elif i == 1:
                        t_grid_data[key+'_'+sts_name[1]] = t_grid_data[key+'_'+sts_name[1]].reshape([grid_lat,grid_lon])
                    elif i == 2:
                        t_grid_data[key+'_'+sts_name[2]] = (t_grid_data[key+'_'+sts_name[2]] / t_grid_data[key+'_'+sts_name[3]])
                        t_grid_data[key+'_'+sts_name[2]] =  t_grid_data[key+'_'+sts_name[2]].reshape([grid_lat,grid_lon])
                    elif i == 3:
                        t_grid_data[key+'_'+sts_name[3]] =  t_grid_data[key+'_'+sts_name[3]].reshape([grid_lat,grid_lon])
                    elif i == 4:
                        t_grid_data[key+'_'+sts_name[4]] = ((t_grid_data[key+'_'+sts_name[4]] / t_grid_data[key+'_'+sts_name[3]].ravel()) - t_grid_data[key+'_'+sts_name[2]].ravel()**2)**0.5
                        t_grid_data[key+'_'+sts_name[4]] =  t_grid_data[key+'_'+sts_name[4]].reshape([grid_lat,grid_lon])
                    elif i == 5:
                        t_grid_data[key+'_'+sts_name[5]] = t_grid_data[key+'_'+sts_name[5]].reshape([grid_lat,grid_lon,bin_num1[key_idx]])
                    elif i == 6:
                        t_grid_data[key+'_'+sts_name[6]+histnames[key_idx]] = t_grid_data[key+'_'+sts_name[6]+histnames[key_idx]].reshape([grid_lat,grid_lon,bin_num1[key_idx],bin_num2[key_idx]])
                key_idx += 1    

            end_time = timeit.default_timer()

            print ("Operation Time in {:7.2f} seconds".format(end_time - start_time))


            #--------------STEP 7:  Create HDF5 file to store the result------------------------------
            l3name  = output_prefix + '.A{:04d}{:02d}'.format(year,month)
            subname = '_baseline_monthly_v8.h5'
            ff=h5py.File(output_dir+l3name+subname,'w')

            PC=ff.create_dataset('lat_bnd',data=map_lat)
            PC.attrs['units']='degrees'
            PC.attrs['long_name']='Latitude_boundaries'    

            PC=ff.create_dataset('lon_bnd',data=map_lon)
            PC.attrs['units']='degrees'
            PC.attrs['long_name']='Longitude_boundaries'    

            for i in range(sts_idx.shape[0]):
                cnt = 0
                for key in t_grid_data:

                    if key.find("1km") != -1: 
                        new_name = key.replace("_1km", "")
                    else: 
                        new_name = key

                    if (sts_name[sts_idx[i]] in key) == True:  
                        #print(sts_name[sts_idx[i]],key,grid_data[key].shape)
                        #print(longname_list[cnt][:20],new_name)
                        #series.addGridEntry(ff,new_name,unit_list[cnt],longname_list[cnt],fillvalue_list[cnt],scale_list[cnt],offst_list[cnt],grid_data[key])
                        series.addGridEntry(ff,new_name,'none',longname_list[cnt],t_grid_data[key])
                        cnt += 1          
            
            ff.close()

            xds = xa.open_dataset(output_dir+l3name+subname)
            print(l3name+subname+' Saved!')

            # convert h5py to xarray

        else:
            raise Exception(f"Unknown operation: '{request}'")
        return xds
