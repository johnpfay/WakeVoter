# -*- coding: utf-8 -*-
"""
VoteSquad.py

Description:
    Development of a voter database consisting of geolocated voter registration data with attached
    information on voting frequency, race, gender, and registered party affiliation as well as the
    census block and voting precinct in which the registered voter resides. 
    
    This database is then aggregated such that clusters of 100 registered black voters within a single
    precinct are identified such that a "super voter" (i.e. a consistent voter) can address these
    other voters efficiently. 
    
Workflow:
    Part 1: Voting data
        Voter registration data, voting history, and address records are subset from the NC State
        Board of Elections (SBE) data files for the specified county. Addresses are joined to the 
        registration data and the dataset is converted into a geospatial dataframe. Then voter 
        history data is summarized to a MECE score and appended to the spatial dataframe. 
        
    Part 2: Census block data
        Census block feature and attribute data are extracted from census servers and combined
        so that each block includes the percent black and percent black over 18. This dataset
        is spatially joined to the voting data features such that each voting record is tagged
        with the census block in which it occurs as well as the block's %age black population.
        
    Part 3: Voting turf designation
        Clusters of voters are grouped and assigned a unique "Turf ID" such that each turf:
        -(mandatory) does not cross precinct lines
        -(mandatory) majority Black
        -(mandatory) At least 50 Black HH
        -Geographically compact (easily walkable area)
        -Practically compact (easily describable area 
         -- e.g. 700-900 blocks of Burch Ave / keeps apartment buildings together)
        -Less than 100 Black HH (this is less imprtant than having at least 50 Black HH)
        -Have at least 2 Black voters who voted in the 2017 municipal election 
        
Requirements: 
    Census API key, saved as APIKey.txt in the project folder.
    
Created: Summer 2019
Creator: John.Fay@duke.edu
"""

#%% IMPORTS
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import speedups
speedups.enable()
import os
import requests, zipfile, io, glob
from datetime import datetime
from pathlib import Path

#%% FUNCTIONS
def get_state_voter_registation_file(NCSBE_folder,overwrite=False):
    '''Returns the file name containing statewide voter registration data. This
    will download the file if it does not exist (or overwrite is set to True)
    
    Args: 
        NCSBE_folder(str): name of folder containing ncvoter_Statewide.txt file
        overwrite(Boolean): whether or not to overwrite existing file (default=False)
        
    Returns:
        filename of state voter registration file
    '''
    #Set the filename 
    state_voter_reg_file = os.path.join(NCSBE_folder,'ncvoter_Statewide.txt')
    #See if the file already exists
    if os.path.exists(state_voter_reg_file) and not(overwrite):
        print(" [{}] file already exists...".format(state_voter_reg_file))
        return state_voter_reg_file
    else:
        #Fetch and unzip the file
        print(" Retrieving address file from NC SBE server [Be patient...]")
        fileURL = 'http://dl.ncsbe.gov/data/ncvoter_Statewide.zip'
        r = requests.get(fileURL)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print("   Unpacking data...")
        z.extractall(NCSBE_folder)
        #Get the file path
        state_voter_reg_file = glob.glob(str(NCSBE_folder)+'/**/ncvoter_Statewide.txt',recursive=True)[0]
        print("   Statewide data stored as\n  [{}]".format(state_voter_reg_file))
        return(state_voter_reg_file)

def get_state_voter_history_file(NCSBE_folder,overwrite=False):
    '''Returns the file name containing statewide voter history data. This
    will download the file if it does not exist.
        
    Args: 
        NCSBE_folder(str): name of folder containing ncvhis_Statewide.txt file
        overwrite(Boolean): whether or not to overwrite existing file (default=False)
        
    Returns:
        filename of state voter history file
    '''
    state_voter_history_file = os.path.join(NCSBE_folder,'ncvhis_Statewide.txt')

    if (os.path.exists(state_voter_history_file) and not(overwrite)):
        print(" [{}] file already exists...".format(state_voter_history_file))
        return state_voter_history_file
    else:
        #Fetch and unzip the file
        print(" Retrieving address file from NC SBE server [Be patient...]")
        #return 
        fileURL = 'http://dl.ncsbe.gov/data/ncvhis_Statewide.zip'
        r = requests.get(fileURL)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print("   Unpacking data...")
        z.extractall(NCSBE_folder)
        #Get the file path
        state_voter_history_file = glob.glob(str(NCSBE_folder)+'/**/ncvhis_Statewide.txt',recursive=True)[0]
        print("   Statewide data stored as\n  [{}]".format(state_voter_history_file))
        return(state_voter_history_file)

def get_county_voter_MECE_data(state_history_file, county_name):
    '''Returns a dataframe of MECE tags for each voter in the county
    
    Description:
        Examines the voter history of each voter and assignes a MECE score based
        on the elections in which he/she voted. Scores are as follows:
        1. Any voter who voted in Oct OR Nov 2017 
        2. Any voter who didn't vote in Oct OR Nov 2017 but voted in  Nov 2018
        3. Any voter who didn't vote in Oct/Nov 2017, didn't vote in Nov 2018, 
           but did vote in Nov 2016
        4. Any voter who didn't vote in Oct/Nov 2017, didn't vote in Nov 2018, 
           didn't vote in Nov 2016, but did vote in 2012
        5. Anyone in the voter-file with no history of voting in Nov 2012, 
           Nov 2016, Oct/Nov 2017, Nov 2018
           
    Args:
        county_name(str): Name of the county to create the table for
        state_voter_history_file(str): location of the voting history file
        
    Returns:
        dataframe of voters, indexed by `ncid`
    '''
    #Read the data into a dataframe
    if state_history_file[-4:] == '.txt':
        print(" Reading in state voter history data")
        dfStateHistory = pd.read_csv(state_history_file,sep='\t',usecols=('county_desc','election_lbl','ncid'))
        #Subset records for the county
        print(" Subseting county records")
        dfCountyHistory = dfStateHistory[dfStateHistory['county_desc']==county_name]
    else:
        print(" Reading in county records")
        dfCountyHistory = pd.read_csv(state_history_file,usecols=('county_desc','election_lbl','ncid'))
    
    #Subset records for the elections of interest
    print(" Subsetting election data")
    elections = ('10/10/2017','11/07/2017','11/06/2018','11/08/2016','11/06/2012')
    dfSubset = dfCountyHistory.loc[dfCountyHistory.election_lbl.isin(elections),:]
    
    #Pivot on these elections
    print(" Determining MECE scores",end="")
    dfPivot = pd.pivot_table(dfSubset,
                             columns = 'election_lbl',
                             index = 'ncid',
                             aggfunc = 'count',
                             fill_value = 0
                            ).droplevel(level=0,axis=1)
    
    #Rename columns
    print(" .",end="")
    dfPivot.rename({'10/10/2017':'Oct17','11/06/2012':'Nov12',
                    '11/06/2018':'Nov18','11/07/2017':'Nov17',
                    '11/08/2016':'Nov16'}, axis=1,inplace=True)
    
    #Create filters
    print(" .",end="")
    e12 = dfPivot.Nov12 == 1
    e16 = dfPivot.Nov16 == 1
    e17 = (dfPivot.Oct17 == 1) | (dfPivot.Nov17 == 1)
    e18 = dfPivot.Nov18 == 1
    
    #Apply filters to assign MECE values
    print(" .")
    dfPivot.loc[e17, "MECE"] = 1
    dfPivot.loc[~e17 & e18, "MECE"] = 2
    dfPivot.loc[~e17 & ~e18 & e16, "MECE"] = 3
    dfPivot.loc[~e17 & ~e18 & ~e16 & e12, "MECE"] = 4
    dfPivot.loc[~e17 & ~e18 & ~e16 & ~e12, "MECE"] = 5
    
    #Return the dataframe
    print(" Returning dataframe")
    return dfPivot

def get_state_address_file(NCSBE_folder):
    '''Returns the file name containing state address data. This will
    download the file if it does not exist.
    '''
    import requests, zipfile, io, glob
    #First, see if the state file has already been retrieved, return if so
    file_list = glob.glob(str(NCSBE_folder)+'/**/address_points_sboe.txt',recursive=True)
    if len(file_list) > 0:
        state_address_file = file_list[0]
        print(" Statewide address file found:\n  [{}]".format(state_address_file))
    else: #Otherwise retrieve the file from the NC SBE server
        print(" Retrieving address file from NC SBE server [Be patient...]")
        fileURL = 'https://s3.amazonaws.com/dl.ncsbe.gov/ShapeFiles/address_points_sboe.zip'
        r = requests.get(fileURL)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print("   Unpacking data...")
        z.extractall(NCSBE_folder)
        #Get the file path
        state_address_file = glob.glob(str(NCSBE_folder)+'/**/address_points_sboe.txt',recursive=True)[0]
        print("   Statewide data stored as\n  [{}]".format(state_address_file))
    return(state_address_file)


def get_county_address_file(county_name, NCSBE_folder):
    '''Creates a local csv file of NC addresses for the county provided.
    
    Args:
        county_name(str): The name of the county to retrieve
        output_folder(str): Folder into which csv file should be saved
        
    Returns:
        dataframe of addresses for the county.
    '''
    import glob
    #Capitalize the county name, for consistency
    county_name = county_name.upper()
    
    #See if the county file already exists, if so create and return a dataframe
    file_list = glob.glob(str(NCSBE_folder)+'/**/address_points_{}.csv'.format(county_name),recursive=True)
    if len(file_list) > 0:
        county_address_file = file_list[0]
        print(" Found county address file:\n  [{}]".format(county_address_file))
        return county_address_file
    else: 
        print(" Building country address file...")
    
    #Get the state address file (this will pull it, if needed)
    print(str(NCSBE_folder))
    print('before function call')
    state_address_file = get_state_address_file(NCSBE_folder)
    #Get the associated metadata file (containing counties)
    state_address_metadata = glob.glob(str(NCSBE_folder)+'/**/address_points_data_format.txt',recursive=True)[0] 
    
    #Generate a list of columns from the metadata file 
    print("...Generating statewide address dataframe...")
    with open(state_address_metadata,'r') as colFile:
        theText = colFile.readlines()
        columns = [theLine.split("\n")[0].split()[0] for theLine in theText[7:]]
    #Read in the data, using the metadata to supply column names
    dfState = pd.read_csv(state_address_file,sep='\t',dtype='str',
                          header=None,index_col=0,names = columns)
    print("   ...{} statewide records loaded".format(dfState.shape[0]))
    #Extract Wake Co addresses and save
    dfCounty = dfState[dfState.county == county_name]
    print("   ...{} county records extracted".format(dfCounty.shape[0]))
    #Save to a file
    county_address_file = state_address_file.replace('sboe.txt','{}.csv'.format(county_name))
    print(" County address file created:\n  [{}]".format(county_address_file))
    dfCounty.to_csv(county_address_file,index=False)
    #Return the dataframe
    return county_address_file
    
def get_voter_data(data_file, address_file, county_name, dfMECE, out_shapefile, overwrite=False):
    '''Creates a geocoded feature class of voters within the selected county.
    
    Description:
        Extracts voter data for the provided county from the NC SBE voter
        registration database (http://dl.ncsbe.gov/data/ncvoter_Statewide.zip), 
        and then "geocodes" the data by joining the registered voter's address
        to SBE's address file (https://dl.ncsbe.gov/index.html?prefix=ShapeFiles/).
        
        Metadata on the source data format is here:
            https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis_ncvoter_data_format.txt
        
    Args:
        data_file(str): Path to local, unzipped voter registration database
        address_file(str): Path to local, unzipped address points file
        county_name(str): Name of county to extract
        dfMECE(dataframe): dataframe of Voter MECE scores
        out_shapefile(str): Name of shapefile in which to save the output
    Returns: 
        geodataframe of addresses
    '''
    #See if the output already exists (and if overwrite is False); if so, create a geodataframe
    if os.path.exists(out_shapefile) and not overwrite:
        print("Output shapefile exists.")
        print("  Creating geodata dataframe from {}\n  [Be patient...]".format(out_shapefile))
        gdfBlocks = gpd.read_file(out_shapefile)
        return(gdfBlocks)

    #Othersiwse read in all the voter registration data
    #load 20,000 rows at a time into an iterator to save memory
    print("  Reading in the voter registration data file...")

    # Expected datatypes for each column
    dtype = {
        'county_desc': np.object,
        'voter_reg_num': np.int64,
        'last_name': np.object,
        'first_name': np.object,
        'middle_name': np.object,
        'res_street_address': np.object,
        'res_city_desc': np.object,
        'state_cd': np.object,
        'zip_code': np.float64,
        'mail_addr1': np.object,
        'mail_city': np.object,
        'mail_state': np.object,
        'mail_zipcode': np.object,
        'full_phone_number': np.object,
        'race_code': np.object,
        'ethnic_code': np.object,
        'gender_code': np.object,
        'birth_age': np.int64,
        'precinct_abbrv': np.object,
        'ncid': np.object
    }
    dataIterator = pd.read_csv(data_file,
                    usecols=['county_desc','voter_reg_num','res_street_address',
                             'res_city_desc','state_cd','zip_code','precinct_abbrv',
                             'race_code','ethnic_code','gender_code','ncid',
                             'mail_addr1','mail_city','mail_state','mail_zipcode',
                             'full_phone_number','birth_age','voter_reg_num',
                             'last_name','first_name','middle_name','precinct_abbrv'],
                    sep='\t',
                    chunksize=20000,
                    dtype=dtype,
                    encoding = "ISO-8859-1",low_memory=True)

    #Filter the selected county and then add it to the dfCounty DataFrame
    print("  Selecting records for {} county...".format(county_name))
    dfCounty = pd.DataFrame()
    for chunk in dataIterator:
        chunk = chunk[chunk['county_desc'] == county_name.upper()]
        dfCounty = pd.concat([dfCounty,chunk])
    print(" {} records selected".format(dfCounty.shape[0]))

    #Drop the county name from the table and set the voter registration # as index
    print("  Tidying data...")
    dfCounty.drop('county_desc',axis=1,inplace=True)
    dfCounty.set_index('voter_reg_num',inplace=True)
    #Drop rows with no address data
    dfCounty.dropna(how='any',inplace=True,
                    subset=['res_street_address','res_city_desc','state_cd','zip_code'])
    #Remove double spaces from the residential address field 
    dfCounty['res_street_address'] = dfCounty['res_street_address'].apply(lambda x: ' '.join(x.split()))
    
    #Read address file into a dataframe
    print("   Reading in address file...")
    dfAddresses = pd.read_csv(address_file,
                              usecols=['st_address','city','zip','latitude','longitude'])
    
    #Join coords to dfVoter
    print("   Joining address to voter data")
    dfX = pd.merge(left=dfCounty,
                   left_on=['res_street_address','res_city_desc','zip_code'],
                   right=dfAddresses,
                   right_on=['st_address','city','zip'],
                   how='left')
    #Drop records that weren't geocoded
    dfX.dropna(axis=0,subset=['longitude','latitude'],inplace=True)
    
    print("  Appending voter MECE scores to voter features")
    dfX2 = pd.merge(dfX,dfMECE,how = 'left',left_on='ncid',right_on='ncid')
    #Update records with no voting history as MECE = 5
    dfX2.loc[dfX2.MECE.isnull(),"MECE"] = 5
    
    #Convert to geodataframe
    print("   Converting to spatial dataframe")
    from shapely.geometry import Point
    geom = [Point(x,y) for x,y in zip(dfX2.longitude,dfX.latitude)]
    gdfVoter = gpd.GeoDataFrame(dfX2,geometry=geom,crs={'init':'epsg:4269'})
    
    #Save the geodataframe
    if out_shapefile == '': return gdfVoter
    
    #Otherwise, save to a file
    print("  Saving to {} [Be patient...]".format(out_shapefile))
    gdfVoter.to_file(out_shapefile,filetype='Shapefile')
    
    #Write projection to .prj file
    with open(out_shapefile[:-3]+'prj','w') as outPrj:
        outPrj.write('GEOGCS["GCS_North_American_1983",'+
                     'DATUM["D_North_American_1983",'+
                     'SPHEROID["GRS_1980",6378137.0,298.257222101]],'+
                     'PRIMEM["Greenwich",0.0],'+
                     'UNIT["Degree",0.0174532925199433]]')
    
    #Write metadata  to .txt file
    current_date = datetime.now().strftime("%Y-%m-%d")
    with open(out_shapefile[:-3]+'txt','w') as outTxt:
        outTxt.write('Voter registration data for {} Co. extracted from\n'.format(county_name))
        outTxt.write('NC SBE: https://www.ncsbe.gov/data-stats/other-election-related-data\n')
        outTxt.write('File created on {}'.format(current_date))
    
    print("[Spatial dataframe now stored as 'gdfVoter']")
    return gdfVoter
        
def get_block_features(st_fips,co_fips,output_shapefile,api_key):
    '''Imports census block features for the supplied county FIPS code
    
    Description:
        Extracts 2010 census block features for the provided county into a 
        geodataframe, retrieves race composition attributes, and then joins
        these attributes (population, black population, population 18+, 
        black population 18+, and percentages) to the blocks. 
        
        Census feature data are from 'https://www2.census.gov/geo/tiger/'.
        
        If a shapefile name is provided, the data are saved to that name. Otherwise,
        just the geodataframe is returned.
    
    Args:
        st_fips(str): State FIPS code (e.g. '37')
        co_fips(str): County FIPS code (e.g. '183')
        df_attributes(dataframe): dataframe of block attributes
        output_shapefile(str)[optional]: Name to save output shapefile
        api_key(str): Census API key
        
    Returns:
        Geodataframe of census blocks for the county with race data
    '''
    #Data URL for fetching data
    dataURL = 'https://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/tabblock2010_{}_pophu.zip'.format(st_fips)
        
    #See if the data have already been pulled; if so, read into dataframe and return
    if os.path.exists(output_shapefile):
        print("  Data already downloaded\n  [{}]".format(output_shapefile))
        print("  ...reading data into a dataframe")
        gdfBlocks = gpd.read_file(output_shapefile)
        return gdfBlocks

    #See if the statewide block shapefile has been downloaded
    stateCensusBlocksFile = './data/Census/StateBlocks.shp'
    if os.path.exists(stateCensusBlocksFile):
        print("  Creating dataframe from existing state block feature class.")
        fcStBlocks = gpd.read_file(stateCensusBlocksFile)
    else:     #Pull the state block data for the supplied FIPS code
        print("   Downloading blocks for state FIPS {}; this take a few minutes...".format(st_fips))
        fcStBlocks = gpd.read_file(dataURL)
        fcStBlocks.to_file(stateCensusBlocksFile)
    
    #Subset county blocks
    print("   Subsetting data for County FIPS {} ".format(co_fips))
    fcCoBlocks = fcStBlocks[fcStBlocks.COUNTYFP10 == co_fips]
    
    #Retrieve block attribute data
    print("   Fetching block attribute data")
    dfAttribs = _get_block_attributes(st_fips,co_fips,api_key)
    fcCoBlocks =  pd.merge(left=fcCoBlocks,left_on='BLOCKID10',
                           right=dfAttribs,right_on='GEOID10',
                           how='outer')
    
    #Add field for number of black households
    fcCoBlocks['BlackHH'] = round(fcCoBlocks.HOUSING10 * fcCoBlocks.PctBlack / 100).astype('int')
    
    #If no output name is given, just return the geodataframe
    if output_shapefile == '': return fcCoBlocks
    
    #Otherwise, save to a file
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
                     dataURL + '\non {}.\n\n'.format(current_date) )
        outTxt.write('The following attributes were collected from\n' +
                     'https://api.census.gov/data/2010/dec/sf1 and joined:\n' +
                     '\tP003001 - Total population\n' +
                     '\tP003003 - Total Black or African American population\n' +
                     '\tP010001 - Total population 18 years and older\n' +
                     '\tP010004 - Total Black/African-American population 18 years or older\n\n')
        outTxt.write('[PctBlack] computed as [P003003] / [P003001] * 100)\n')
        outTxt.write('[PctBlack18] computed as [P010004] / [P010001] * 100)\n')
        outTxt.write('[BlackHH] computed as [HOUSING10] * [PctBlack]), rounded to the nearest integer')
        
    #Return the geodataframe
    return fcCoBlocks
        
def _get_block_attributes(st_fips,co_fips,api_key):
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
    print("   ...downloading data from {}".format(theURL))
    response = requests.get(theURL,params) 
    response_json = response.json()
    #Convert JSON to pandas dataframe
    print("   ...cleaning data...")
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
    dfData.fillna(0,inplace=True)
    #Remove GEOID component columns
    dfData.drop(['state','county','tract','block'],axis='columns',inplace=True)

    #Return the dataframe
    return dfData

def append_blockdata_to_voterpoints(gdf_voter,gdf_blocks,output_shapefile):
    '''Spatially joins block attributes to each voter point. Saves the result 
    the feature class provided. 
    
    Args: 
        gdf_voter(geodataframe): geo dataframe of voter points
        gdf_blocks(geodataframe): geo dataframe of census blocks
        
    Returns:
        geodataframe of voter points with Block attributes.
    '''
    print("  Performing spatial join")
    gdf = gpd.sjoin(gdf_voter,gdf_blocks,how='left',op='within')
    print("  ...removing extraneous columns")
    gdf.drop(columns=['index_right','STATEFP10', 'COUNTYFP10', 'TRACTCE10', 
                      'BLOCKCE', 'GEOID10', 'PARTFLG'],axis='columns',inplace=True)
    
    #Save dataframe, if one is given
    if output_shapefile:
        #Save to shapfile, if extension is ".shp"
        if output_shapefile[-4:] == '.shp':
            print("  Saving geodataframe as shapefile. [Be patient...]")
            gdf.to_file(output_shapefile,format='shapefile')
        else: #Otherwise, save to csv
            print("  Saving geodataframe as CSV file...")
            gdf.to_csv(output_shapefile,index_label="OID")
        
        #Write metadata  to .txt file
        print('   ...writing metadata.')
        current_date = datetime.now().strftime("%Y-%m-%d")
        with open(output_shapefile[:-3]+'txt','w') as outTxt:
            outTxt.write('Voter registration and history data for {} Co. extracted from\n'.format(county_name))
            outTxt.write('NC SBE: https://www.ncsbe.gov/data-stats/other-election-related-data\n')
            outTxt.write('Census block data appended to points.\n\n')
            outTxt.write('File created on {}'.format(current_date))
            
    return gdf
    
def subset_voter_points(gdf_voters,output_shapefile=''):
    '''Returns a dataframe of just black voters in blocks that are majority black
    
    Args:
        gdf_voters(geodataframe): Geodataframe of all voter points
        output_shapefile(str): File name for feature class to save [optional]
    Returns:
        geodataframe of black voters within majority black blocks
    '''
    mask_Voter = gdf_voters['race_code'] == 'B'
    mask_Block = gdf_voters['PctBlack'] >= 50
    gdf = gdf_voters.loc[mask_Voter & mask_Block]
    if output_shapefile:
        #Save to shapfile, if extension is ".shp"
        if output_shapefile[-4:] == '.shp':
            print("  Saving geodataframe as shapefile...")
            gdf.to_file(output_shapefile,format='shapefile')
        else: #Otherwise, save to csv
            print("  Saving geodataframe as CSV file...")
            gdf.to_csv(output_shapefile,index_label="OID")
            
        #Write metadata  to .txt file
        print('   ...writing metadata.')
        current_date = datetime.now().strftime("%Y-%m-%d")
        with open(output_shapefile[:-3]+'txt','w') as outTxt:
            outTxt.write('Subset of voter points\n'.format(county_name))
            outTxt.write('Voting points for only black voter in majority black blocks\n\n')
            outTxt.write('File created on {}'.format(current_date))
                    
    return gdf

def tally_block_MECE_scores(gdf_voter):
    '''Adds count of each MECE scores to each census blocks.
    
    Args:
        gdf_voter(dataframe): geodataframe of voter points. Must have block attributes.
        
    Returns:
        geodataframe of blocks with columns for each MECE catagory and totals.
    '''
    #Ensure gdf_voter has the right columns
    if not ("MECE" in gdf_voter.columns):
        print(' ERROR: [MECE] not found in supplied dataframe.')
        return None
    #Pivot the voter data on MECE scores, tallying counts
    print("  Pivot data on MECE values")
    df_MECE = (gdf_voter.pivot_table(index='BLOCKID10',
                                    columns='MECE',
                                    aggfunc={'ncid':'count'})
               .fillna(0)               # Set NaNs to zero
               .droplevel(0,axis=1)     # Drop multi-index
               .reset_index())          # Reset row index
    #Subset columns
    print("  Removing extraneous columns")
    df_MECE.columns = ['BLOCKID10','MECE1','MECE2','MECE3','MECE4','MECE5']
    #Compute total voters in the block
    print("  Computing total election count per voter")
    df_MECE['Total']=df_MECE[['MECE1','MECE2','MECE3','MECE4','MECE5']].sum(axis=1)
    #Convert dtypes to integers
    colList = ['MECE1','MECE2','MECE3','MECE4','MECE5','Total']
    df_MECE[colList] = df_MECE[colList].astype('int')

    return df_MECE

#%% main
#Set the run time variables
state_fips = '37'
county_fips  = '183'
county_name = 'WAKE'

#Set structure variables
NCSBE_folder = Path('data/NCSBE')    #Folder containing NC SBE data
CENSUS_folder = Path('data/Census')  #Folder containing Census data

#Create a folder to hold county data
COUNTY_folder = Path('data/{}'.format(county_name))
if not(os.path.exists(COUNTY_folder)):
    os.mkdir(COUNTY_folder)

#Set the output filenames
voter_shapefile_name = os.path.join(COUNTY_folder,'{}_voter_points.shp'.format(county_name))
subset_voter_shapefile_name = os.path.join(COUNTY_folder,'{}_voter_subset_points.shp'.format(county_name))
block_shapefile_filename = os.path.join(COUNTY_folder,'{}_blocks.shp'.format(county_name))
orgunits_shapefile_filename = os.path.join(COUNTY_folder,'{}_orgunits.shp'.format(county_name))

#%% PART 1. GET AND PROCESS VOTING DATA
#Get the NC SBE voter registration and history files for the county 
print("1a. Getting statewide voting registration data".format(county_name))
state_voter_reg_file = get_state_voter_registation_file(NCSBE_folder)

print("1b. Getting voting history data for {} county".format(county_name))
state_voter_history_file = get_state_voter_history_file(NCSBE_folder)
#county_voter_history_file = get_county_voter_history_file(state_voter_history_file)

print("1c. Computing MECE scores for {} voters".format(county_name))
dfVoterMECE = get_county_voter_MECE_data(state_voter_history_file,county_name)

#Get the file of NC SBE address s for the state (if needed) and then the county subset
print("1d. Getting address data for {} county".format(county_name))
county_address_file = get_county_address_file(county_name, NCSBE_folder)

#Retrieve voter features
print("1e. Converting voting data to spatial features")
gdfVoter1 = get_voter_data(state_voter_reg_file,
                          county_address_file,
                          county_name,
                          dfVoterMECE,"")
#%% PART 2. CENSUS DATA

#Get the Census API key
print('2a. Getting the census API key')
censusKey = open("APIkey.txt","r").readline().strip()
print("This product uses the Census Bureau Data API but ")
print("is not endorsed or certified by the Census Bureau.")

#Get the Census block features and attributes for the county
print('2b. Fetching/reading census block features')
gdfBlocks = get_block_features(state_fips,county_fips,block_shapefile_filename,censusKey)

#Join blocks to voter points
print('2c. Joining block data to voter features')
gdfVoter = append_blockdata_to_voterpoints(gdfVoter1,gdfBlocks,'')#voter_shapefile_name)

#Subset voter points
print('2d. Extracting black voters in majority black blocks')
gdfVoter_subset = subset_voter_points(gdfVoter,'')#subset_voter_shapefile_name)

#Compute MECE counts by block
dfMECE = tally_block_MECE_scores(gdfVoter_subset)
#%% PART 3. ASSIGN VOTER TURF VALUES TO VOTING POINTS
# Organizational units are areas managed by one or two 'super voters'.
# These areas should:
#  (1) have need, defined as having a certian number of black voters and
#      occur within a block that is majority black, in terms of households
#  (2) have leaders, defined as having two "MECE 1" voters
#  (3) be of manageable size, defined as fewer than 100 households
#
# The workflow here is:
# 1. Remove blocks that are not majority black (pctBlack < 50)
# 2. Join MECE data to the majority black geodataframe
# 3. Subset blocks that have at least 50 black HH, keep as "Original Block" org units
# 4. Of those blocks that remain, find which, if clustered, yield at least 50 black HH
#  4a. Spatially cluster (unary_union) blocks with fewer than 50 black HH 
#      * The clustering algorithm clusters adjacent blocks until 100 black HH occur within them
#  4b. Recalculate voter statistics on clustered blocks
#  4c. Remove clustered blocks still with fewer than 50 black HH
#  4d. Subset clustered blocks with more than 50 black HH, but fewer than 100; 
#      Keep these as "Full block cluster" org units
#  4e. With block clusters with 100 or more black HH, cluster individual blocks
#      one at a time until 100 black HH are reached, then restart with a new subset
#      of blocks in the cluster until all blocks have been processed. Keep these
#      as "Partial block clusters". 
# 5. Combine the three org unit layers: "Original Block" (step 3), "Full block 
#    clusters" (step 4d), and "Partial block clusters" (step 4e).
# 6. Assign random IDs to all org units
# 7. Compute area (sq mi) of org unit features.
# 8. Tag voter data with orgunit ID.
# 9. Add precinct and city information to org unit
# 10. Tidy and export voter and org unit feature classes

print("PROCESSING ORG UNITS")

#--- Step 1. Select blocks that are majority black and add MECE count data
print(" 1. Subsetting blocks that are majority black.")
gdfMajBlack = gdfBlocks.query('PctBlack >= 50')

#--- Step 2. Join MECE data (and tidy up fields)
print(" 2. Joining block MECE data to selected blocks.")
gdfMajBlack = pd.merge(gdfMajBlack,dfMECE,on='BLOCKID10',how='left').fillna(0)
# Fix dtypes (Pandas defaults back to floats)
gdfMajBlack[gdfMajBlack.columns[-6:]] = gdfMajBlack[gdfMajBlack.columns[-6:]] .astype('int')
gdfMajBlack.drop(['STATEFP10','COUNTYFP10','TRACTCE10','BLOCKCE','PARTFLG'],
                 axis=1,inplace=True)

#--- Step 3. Subset majority black blocks with > 50 black HH and save as gdf_Org1 
#  to be merged with other org units later.
print(" 3. Keeping majority black blocks with > 50 black households to 'Org1'")
gdf_Org1 = gdfMajBlack.query('BlackHH > 50').reset_index()
gdf_Org1.drop(['index', 'BLOCKID10','GEOID10'],axis=1,inplace=True)
gdf_Org1['OrgID'] = gdf_Org1.index + 1
gdf_Org1['OrgType'] = 'block'

#--- Step 4. Select the majority black blocks with fewer than 50 black HH for clustering
print(" 4. Clustering the remaining blocks...")
gdfMajBlack_LT50 = gdfMajBlack.query('BlackHH < 50')

#Step 4a. Cluster adjacent blocks into a single feature and assing a ClusterID
print("  4a. Finding intitial clusters...")
gdfClusters = gpd.GeoDataFrame(geometry = list(gdfMajBlack_LT50.unary_union))
gdfClusters['ClusterID'] = gdfClusters.index
gdfClusters.crs = gdfMajBlack_LT50.crs #Set the coordinate reference system

#Step 4b. Recalculate population stats for the clusters
print("  4b. Computing number of black households in new clusters...")
# -> Done by first spatially joininig the cluster ID to the blocks w/ < 50 Black HH
gdfMajBlack_LT50_2 = gpd.sjoin(gdfMajBlack_LT50,gdfClusters,
                               how='left',op='within').drop("index_right",axis=1)
# -> Next we dissolve on the cluster ID computing SUM of the numeric attributes
#    and updating the percentage fields
gdfClusters_2 = gdfMajBlack_LT50_2.dissolve(by='ClusterID', aggfunc='sum')
gdfClusters_2['PctBlack'] = gdfClusters_2['P003003'] / gdfClusters_2['P003001'] * 100
gdfClusters_2['PctBlack18'] = gdfClusters_2['P010004'] / gdfClusters_2['P010001'] * 100

#Step 4c. Remove block clusters with fewer than 50 BHH; these are impractical
print("  4c. Removing clusters still with < 50 black households (impractical)...")
gdfClusters_2 = gdfClusters_2.query('BlackHH >= 50')

#Step 4d. Select clusters with fewer than 100 BHH and save as gdf_Org2, to be merged...
print("  4d. Keeping new clusters with fewer than 100 black households: 'Org2'")
gdf_Org2 = gdfClusters_2.query('BlackHH <= 100').reset_index()
gdf_Org2['OrgID'] = gdf_Org1['OrgID'].max() + gdf_Org2.index + 1
gdf_Org2['OrgType'] = 'block aggregate'

#Step 4e. For clusters that are too big (> 100 Black HH), cluster incrementally
#  so that clusters have up to 100 Black HH. These will be saved as gdf_Org3
print("  4e. Reclustering clusters with > 100 HH into smaller aggregates...")
#-> Get a list of Cluster IDs for block clusters with more than 100 BHH;
#   we'll cluster individual blocks with these IDs until BHH >= 100
clusterIDs = gdfClusters_2.query('BlackHH > 100').index.unique()

#Iterate through each clusterID
gdfs = []
for clusterID in clusterIDs:
    #Get all the blocks in the selected cluster
    gdfBlksAll = gdfMajBlack_LT50_2.query('ClusterID == {}'.format(clusterID)).reset_index()
    #Assign the X coordinate, used to select the first feature in a sub-cluster
    gdfBlksAll['X'] = gdfBlksAll.geometry.centroid.x
    #Set all blocks to "unclaimed"
    gdfBlksAll['claimed'] = 0
    #Determine how many blocks are unclaimed
    unclaimedCount = gdfBlksAll.query('claimed == 0')['X'].count()
    #Initialize the loop catch variable
    stopLoop = 0 
    #Run until all blocks have been "claimed"
    while unclaimedCount > 0:
        
        #Extract all unclaimed blocks
        gdfBlks = gdfBlksAll[gdfBlksAll.claimed == 0].reset_index()

        #Get the initial block (the western most one); get its BHH and geometry
        gdfBlock = gdfBlks[gdfBlks.X == gdfBlks.X.min()]
        BHH = gdfBlock.BlackHH.sum()
        geom = gdfBlock.geometry.unary_union
        
        #Expand the geometry until 100 BHH are found
        stopLoop2 = 0 #Loop break check
        while BHH < 100:
            #Select unclaimed blocks that within the area
            gdfNbrs = gdfBlksAll[(gdfBlksAll.touches(geom))]
            gdfBoth = pd.concat((gdfBlock,gdfNbrs),axis='rows',sort=False)
            gdfBlock = gdfBoth.copy(deep=True)
            #Tally the BHHs in the area and update the area shape
            BHH = gdfBoth.BlackHH.sum()
            geom = gdfBoth.geometry.unary_union
            #Catch if run 100 times without getting to 100 BHH
            stopLoop2 += 1
            if stopLoop2 > 100: 
                print("BHH never reached 100")
                break
                
        #Extract features intersecting the geometry to a new dataframe
        gdfSelect = (gdfBlksAll[(gdfBlksAll.centroid.within(geom)) & 
                                (gdfBlksAll.claimed == 0) 
                               ]
                 .reset_index()
                 .dissolve(by='ClusterID', aggfunc='sum')
                 .drop(['level_0','index','X'],axis=1)
                )
        
        #Set all features intersecting the shape as "claimed"
        gdfBlksAll.loc[gdfBlksAll.geometry.centroid.within(geom),'claimed'] = 1
        unclaimedCount = gdfBlksAll.query('claimed == 0')['X'].count()

        #Add the dataframe to the list of datarames
        gdfs.append(gdfSelect[gdfSelect['BlackHH'] >= 50])    
        
        #Stop the loop if run for over 100 iterations
        stopLoop += 1
        if stopLoop > 100: break
    
#-> Concat these to a new geodataframe, update pct fields, and add Org ID and types
print("    ...completing creating on new clusters: 'Org3'")
gdf_Org3 = pd.concat(gdfs,sort=False)
gdf_Org3['PctBlack'] = gdf_Org3['P003003'] / gdf_Org3['P003001'] * 100
gdf_Org3['PctBlack18'] = gdf_Org3['P010004'] / gdf_Org3['P010001'] * 100
gdf_Org3['OrgID'] = gdf_Org2['OrgID'].max() + gdf_Org3.index + 1
gdf_Org3['OrgType'] = 'block aggregate'
gdf_Org3.drop(['claimed'],axis=1,inplace=True)

#--- Step 5. Merge all three keepers
print(" 5. Combining Org1, Org2, Org3 into a single feature class")
gdfAllOrgs = pd.concat((gdf_Org1, gdf_Org2, gdf_Org3),axis=0,sort=True)

#--- Step 6. Assign random IDs 
print(" 6. Assigning random IDs for org units")
# 1. Compute Random Org IDs
numRows = gdfAllOrgs.shape[0]
gdfAllOrgs['Rando'] = np.random.randint(numRows,size=(numRows,1)) 
gdfAllOrgs.sort_values(by='Rando',axis=0,inplace=True)
gdfAllOrgs.reset_index(inplace=True)
gdfAllOrgs['RandomID'] = gdfAllOrgs.index + 1
gdfAllOrgs.drop(['index','ClusterID','Rando'],axis=1,inplace=True)

#--- Step 7. Compute org unit area, in square miles
print(" 7. Computing org unit areas (in sq miles)")

## FIX FOR PYPROJ GLITCH ##
import os, sys
env_folder = os.path.dirname(sys.executable)
os.environ['PROJ_LIB']=os.path.join(env_folder,'Library','share')

# Project data to NC State Plane (feet)   
gdfNCStatePlane = gdfAllOrgs.to_crs({'init': 'epsg:2264'})  
# Compute area, in square miles
gdfNCStatePlane['area'] = gdfNCStatePlane.geometry.area 
gdfAllOrgs['sq_miles']  =  gdfNCStatePlane['area'] / 27878400  #ft to sq mi

#--- Step 8. Tag voter data with org Unit ID
print(" 8. Tagging voter data with org unit [random] IDs")
# Spatially join org units' RandomID values to voter points
gdfVoter_org = gpd.sjoin(left_df = gdfVoter, right_df=gdfAllOrgs[['RandomID','geometry']], 
                         how='right',op='within')
# Clean up columns
gdfVoter_org.drop(columns=['index_left','Oct17', 'Nov12', 'Nov18', 'Nov17', 'Nov16',
                           'HOUSING10', 'POP10', 'P003001', 'P003003', 'P010001',
                           'P010004', 'PctBlack', 'PctBlack18', 'BlackHH'],inplace=True)
   
#--- Step 9. Add precinct and city information to org unit
print(" 9. Adding precinct and city information to org units")
# -> Create a lookup table of precincty and city for each random ID
dfLookup = (gdfVoter_org[['RandomID','precinct_abbrv','res_city_desc']].
            groupby('RandomID').
            agg(pd.Series.mode)).reset_index()
# -> Join back to the orgs dataframe
gdfAllOrgs2 = gdfAllOrgs.merge(dfLookup,on='RandomID',how='left')
# -> fix column types
gdfAllOrgs2['precinct_abbrv'] = gdfAllOrgs2['precinct_abbrv'].astype('str')
gdfAllOrgs2['res_city_desc'] = gdfAllOrgs2['res_city_desc'].astype('str')



#--- Step 10. Tidy up and export the org unit feature class
print(' 10. Tiding and exporting org unit features...')
## Rename columns:
gdfAllOrgs2.rename(columns={'precinct_abbrv':'Precinct',
                            'P003001':'Total_census_population',
                            'P003003':'Total_census_Black_population',
                            'PctBlack':'Pct_Black_census',
                            'Total':'Total_Black_registered_population',
                            'sq_miles':'square_miles',
                            'res_city_desc':'city'},inplace=True)
        
## Reorder and subset existing columns
gdfAllOrgs_out = gdfAllOrgs2.loc[:,['RandomID','OrgType','Precinct','BlackHH',
                                    'Total_census_population','Total_census_Black_population',
                                    'Pct_Black_census','Total_Black_registered_population',
                                    'square_miles','MECE1','MECE2','MECE3','MECE4','MECE5',
                                    'city','geometry' ]]

##Append new blank columns
for newCol in ("support_volunteer_name", "support_vol_phone", "support_vol_email", 
               "block_team_member", "block_team-phone", "block_team_email","Notes"):
    gdfAllOrgs_out[newCol]=''    
        
##Write output
gdfAllOrgs_out.to_file(orgunits_shapefile_filename)
gdfAllOrgs_out.drop(['geometry'],axis=1).to_csv(orgunits_shapefile_filename[:-3]+'csv',index=False)

##Write metdatada
with open(orgunits_shapefile_filename[:-4]+"README.txt",'w') as meta:
    meta.write('''Organizational Voting Units.
These are Census blocks that are majority black and have at least 50 black households (BHH).
Adjacent census blocks with fewer than 50 BHH are aggregated together until 100 BHH are found.

Data dictionary:
    'RandomID' - Randomized org unit ID
    'OrgType' - Org type (block or block aggregate)
    'Precinct' - Precinct number
    'BlackHH' - Estimated Black HH
    'Total_census_population' - Total census population
    'Total_census_Black_population' - Total census Black population
    'Pct_Black_census' - --% Black pop (census)
    'Total_Black_registered_population' - Total Black registered population
    'square_miles' - Area of unit in square miles
    'MECE1' - # of black voters in MECE1
    'MECE2' - # of black voters in MECE2
    'MECE3' - # of black voters in MECE3
    'MECE4' - # of black voters in MECE4
    'MECE5' - # of black voters in MECE5
    'city' - City in which majority of org unit is found
    'support_volunteer_name' - 
    'support_vol_phone' -  
    'support_vol_email' - 
    'block_team_member' - 
    'block_team-phone', - 
    'block_team_email' - 
    'Notes'  - 
    
    ''')
print('    Org units saved to {}'.format(orgunits_shapefile_filename))

#--- 11. Tidy and export voter features
print(' 11. Tiding and exporting voter features...')
## Rename columns:
gdfVoter_org_copy = gdfVoter_org.copy(deep=True)
gdfVoter_org_copy = gdfVoter_org.rename(columns={'gender_code':'Gender',
                                                'race_code':'Race',
                                                'res_street_address':'Residential_street_address',
                                                'zip_code':'Residential_street_address_zip',
                                                'mail_addr1':'Mailing_street_address',
                                                'mail_zipcode':'Mailing_street_address_zip',
                                                'birth_age':'Age'})
## Compute address lines
gdfVoter_org_copy['Residential_street_address_line2'] = gdfVoter_org_copy['res_city_desc'] +', '+ gdfVoter_org_copy['state_cd']
gdfVoter_org_copy['Mailing_street_address_line2'] = gdfVoter_org_copy['mail_city'] +', '+ gdfVoter_org_copy['mail_state']
                                                
## Reorder and subset existing columns
gdfVoter_out = gdfVoter_org_copy.loc[:,['RandomID','Gender','Race','Age','MECE','first_name','middle_name',
                                        'last_name','Residential_street_address',
                                        'Residential_street_address_line2','Residential_street_address_zip',
                                        'Mailing_street_address','Mailing_street_address_line2',
                                        'Mailing_street_address_zip','ncid',
                                        'latitude','longitude','geometry']]

##Write output
gdfVoter_out.to_file(voter_shapefile_name)
gdfVoter_out.drop(['geometry'],axis=1).to_csv(voter_shapefile_name[:-3]+'csv',index=False)

##Write metdatada
with open(voter_shapefile_name[:-4]+"README.txt",'w') as meta:
    meta.write('''Organizational Voting Units.
These are Census blocks that are majority black and have at least 50 black households (BHH).
Adjacent census blocks with fewer than 50 BHH are aggregated together until 100 BHH are found.

Data dictionary:
    'RandomID' - Randomized Org Unit ID
    'Gender' - Voter gender 
    'Race' - Voter race
    'Age' - Voter age
    'MECE' - Voter MECE
    'first_name' 
    'middle_name'
    'last_name'
    'Residential street address'
    'Residential street address line 2'
    'Residential street address zip'
    'Mailing street address'
    'Mailing street address line 2'
    'Mailing street address zip'
    'ncid'
    'voter_reg_num'
    'Latitude'
    'Longitude'   
    ''')
        
print('    Voter data saved to {}'.format(voter_shapefile_name))
