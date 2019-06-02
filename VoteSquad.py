# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#%% IMPORTS
import pandas as pd
import geopandas as gpd
import os
import requests
from datetime import datetime

#%% FUNCTIONS
def get_block_features(st_fips,co_fips,output_shapefile):
    '''Imports census block features for the supplied county FIPS code
    
    Args:
        st_fips(str): State FIPS code (e.g. '37')
        co_fips(str): County FIPS code (e.g. '183')
        
    Returns:
        geodataframe of census blocks for the county
    '''
    #Pull the state block data for the supplied FIPS code
    print("Downloading blocks for {}; this take a few minutes...".format(st_fips))
    dataURL = 'https://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/tabblock2010_{}_pophu.zip'.format(st_fips)
    fcStBlocks = gpd.read_file(dataURL)
    
    #Subset county blocks
    print("Subsetting data for County FIPS ".format(co_fips))
    fcCoBlocks = fcStBlocks[fcStBlocks.COUNTYFP10 == co_fips]
    
    #Save to a file
    print("Saving to {}".format(output_shapefile))
    fcCoBlocks.to_file(output_shapefile,filetype='Shapefile')
    
    #Write projection to .prj file
    with open(output_shapefile[:-3]+'prj','w') as outPrj:
        outPrj.write('GEOGCS["GCS_North_American_1983",'+
                     'DATUM["D_North_American_1983",'+
                     'SPHEROID["GRS_1980",6378137.0,298.257222101]],'+
                     'PRIMEM["Greenwich",0.0],'+
                     'UNIT["Degree",0.0174532925199433]]')
    
    #Write metadata  to .txt file
    current_date = datetime.now().strftime("%Y-%m-%d")
    with open(output_shapefile[:-3]+'txt','w') as outTxt:
        outTxt.write('Census block data for FIPS{}{} extracted from\n'.format(st_fips,co_fips) +
                     dataURL + '\n' + 
                     'on {}'.format(current_date)
                     )
        
    #Return the geodataframe
    return fcCoBlocks
        
def get_block_attributes(st_fips,co_fips,output_csv,api_key):
    '''Retrieves race composition data using the Census API
    
    Description: Pulls the following block level data from the 2010 SF1 file:
        P003001 - Total population 
        P003003 - Total Black or African American population
        P010001 - Total population 18 years and older
        P010004 - Total Black/African-American population 18 years or older
    Then compute pct Black and pct Black (18+) columns along with 
    
    Args:
        st_fips(str): State FIPS code (e.g. '37')
        co_fips(str): County FIPS code (e.g. '183')
        output_csv(str): Filename to save data
        api_key(str): Census API key
        
    Returns:
        geodataframe of census blocks for the county
    '''
   #Census API call to get the data for the proivded state/county 
    theURL = 'https://api.census.gov/data/2010/dec/sf1'
    params = {'get':'P003001,P003003,P010001,P010004',
              'for':'block:*',
              'in':'state:{}%county:{}'.format(st_fips,co_fips),
              'key':api_key
         }
    #Send the request and convert the response to JSON format
    print("...downloading data...")
    response = requests.get(theURL,params) 
    response_json = response.json()
    #Convert JSON to pandas dataframe
    print("...cleaning data...")
    dfData = pd.DataFrame(response_json[1:],columns=response_json[0])
    #Convert block data columns to numeric
    floatColumns = ['P003001','P003003','P010001','P010004']
    dfData[floatColumns] = dfData[floatColumns].apply(pd.to_numeric)
    #Combine columns into single GEOID10 attribute
    dfData['GEOID10'] = dfData.state+dfData.county+dfData.tract+dfData.block
    #Compute percentages
    dfData['PctBlack'] = dfData.P003003 / dfData.P003001 * 100
    dfData['PctBlack18'] = dfData.P010004 / dfData.P010001 * 100
    #Set null values to zero
    dfData.fillna(0)
    #Remove GEOID component columns
    dfData.drop(['state','county','tract','block'],axis='columns',inplace=True)
    #Export the relevant columns to a csv file
    print("...saving data to {}".format(output_csv))
    dfData.to_csv(output_csv,index=False)
    #Return the dataframe
    return dfData
    

#%% main
#Set the run time variables
state_fips = '37'
county_fips  = '183'

#Get the Census API key
censusKey = pd.read_csv('{}/APIkeys.csv'.format(os.environ['localappdata'])).iloc[0,1]

#Get the Census block features and attributes
#gdfBlocks = get_block_features(state_fips,county_fips,'scratch/wake_blocks.shp')
dfBlocks = get_block_attributes(state_fips,county_fips,'scratch/wake_attributes.csv',censusKey)

dfBlocks.dtypes
#Join the attributes to the features
dfBlocks[['P003001','P003003','P010001','P010004']] = dfBlocks[['P003001','P003003','P010001','P010004']].apply(pd.to_numeric)
