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
import pandas as pd
import geopandas as gpd
from shapely import speedups
speedups.enable()
import os
import requests, zipfile, io, glob
from datetime import datetime

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
        state_voter_reg_file = glob.glob(NCSBE_folder+'/**/ncvoter_Statewide.txt',recursive=True)[0]
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
        return 
        fileURL = 'http://dl.ncsbe.gov/data/ncvhis_Statewide.zip'
        r = requests.get(fileURL)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print("   Unpacking data...")
        z.extractall(NCSBE_folder)
        #Get the file path
        state_voter_history_file = glob.glob(NCSBE_folder+'/**/ncvhis_Statewide.txt',recursive=True)[0]
        print("   Statewide data stored as\n  [{}]".format(state_voter_history_file))
        return(state_voter_history_file)

def Xget_county_voter_registation_file(state_registration_file):
    '''Returns the file name containing county voter registration data. This
    will create the file if it does not exist. 
    '''
    county_voter_reg_file = './data/WAKE/ncvoter_Wake.csv'
    return county_voter_reg_file

def Xget_county_voter_history_file(state_history_file):
    '''Returns the file name containing county voter history data. This
    will download the file if it does not exist.
    '''
    county_voter_history_file = './data/WAKE/ncvhis_Wake.csv'
    if not os.path.exists(county_voter_history_file):
        print("{} not found.".format(county_voter_history_file))
        return
    return county_voter_history_file

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
    file_list = glob.glob(NCSBE_folder+'/**/address_points_sboe.txt',recursive=True)
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
        state_address_file = glob.glob(NCSBE_folder+'/**/address_points_sboe.txt',recursive=True)[0]
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
    file_list = glob.glob(NCSBE_folder+'/**/address_points_{}.csv'.format(county_name),recursive=True)
    if len(file_list) > 0:
        county_address_file = file_list[0]
        print(" Found country address file:\n  [{}]".format(county_address_file))
        return county_address_file
    else: 
        print(" Building country address file...")
    
    #Get the state address file (this will pull it, if needed)
    state_address_file = get_state_address_file(NCSBE_folder)
    #Get the associated metadata file (containing counties)
    state_address_metadata = glob.glob(NCSBE_folder+'/**/address_points_data_format.txt',recursive=True)[0] 
    
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
    #Pull the state block data for the supplied FIPS code
    print("   Downloading blocks for state FIPS {}; this take a few minutes...".format(st_fips))
    dataURL = 'https://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/tabblock2010_{}_pophu.zip'.format(st_fips)
    fcStBlocks = gpd.read_file(dataURL)
    
    #Subset county blocks
    print("   Subsetting data for County FIPS {} ".format(co_fips))
    fcCoBlocks = fcStBlocks[fcStBlocks.COUNTYFP10 == co_fips]
    
    #Retrieve block attribute data
    print("   Fetching block attribute data")
    dfAttribs = get_block_attributes(st_fips,co_fips,api_key)
    fcCoBlocks =  pd.merge(left=fcCoBlocks,left_on='BLOCKID10',
                           right=dfAttribs,right_on='GEOID10',
                           how='outer')
    
    
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
                     dataURL + '\n' + 
                     'on {}'.format(current_date)
                     )
        
    #Return the geodataframe
    return fcCoBlocks
        
def get_block_attributes(st_fips,co_fips,api_key):
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
    
def get_voter_data(data_file, address_file, county_name, out_shapefile, overwrite=False):
    '''Creates a geocoded feature class of voters within the selected county.
    
    Description:
        Extracts voter data for the provided county from the NC SBE voter
        registration database (http://dl.ncsbe.gov/data/ncvoter_Statewide.zip), 
        and then "geocodes" the data by joining the registered voter's address
        to SBE's address file (https://dl.ncsbe.gov/index.html?prefix=ShapeFiles/).
        
    Args:
        data_file(str): Path to local, unzipped voter registration database
        address_file(str): Path to local, unzipped address points file
        county_name(str): Name of county to extract
        out_shapefile(str): Name of shapefile in which to save the output
    Returns: 
        geodataframe of addresses
    '''
    #See if the output already exists (and if overwrite is False); if so, create a geodataframe
    if os.path.exists(out_shapefile) and not overwrite:
        print("Output shapefile exists.\nCreating geodata dataframe from {}\n[Be patient...]".format(out_shapefile))
        gdfBlocks = gpd.read_file(out_shapefile)
        return(gdfBlocks
               )
    #Othersiwse read in all the voter registration data
    print("  Reading in the voter registration data file...")
    dfAll = pd.read_csv(data_file,
                    usecols=['county_desc','voter_reg_num','res_street_address',
                             'res_city_desc','state_cd','zip_code','precinct_abbrv','race_code',
                             'ethnic_code','gender_code','party_cd','ncid'],
                    sep='\t',
                    encoding = "ISO-8859-1")
    
    #Select records for the provided county name - into a new dataframe
    print("  Selecting records for {} county...".format(county_name),end='')
    dfCounty = dfAll[dfAll['county_desc'] == county_name.upper()].reindex()
    print(" {} records selected".format(dfCounty.shape[0]))
    
    #Remove dfAll to free memory
    del(dfAll)
    
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
    
    #Convert to geodataframe
    print("   Converting to spatial dataframe")
    from shapely.geometry import Point
    geom = [Point(x,y) for x,y in zip(dfX.longitude,dfX.latitude)]
    gdfVoter = gpd.GeoDataFrame(dfX,geometry=geom,crs={'init':'epsg:4269'})
    
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


#%% main
#Set the run time variables
state_fips = '37'
county_fips  = '183'
county_name = 'WAKE'

#Set structure variables
NCSBE_folder ='.\\data\\NCSBE'     #Folder containing NC SBE data
CENSUS_folder = '.\\data\\Census'  #Folder containign Census data

#Create a folder to hold county data
COUNTY_folder = '.\\data\\{}'.format(county_name)

#Set the output filenames
voter_shapefile_name = os.path.join(COUNTY_folder,'{}_voter_points.shp'.format(county_name))
block_shapefile_filename = os.path.join(COUNTY_folder,'{}_blocks.shp'.format(county_name))

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
county_address_file = get_county_address_file(county_name,NCSBE_folder)

#Retrieve voter features
print("1e. Converting voting data to spatial features")
gdfVoter = get_voter_data(state_voter_reg_file,county_address_file,county_name,voter_shapefile_name)

#Append voter summary data
print("1f. Appending voter MECE scores to voter features")
gdfVoter = pd.merge(gdfVoter,dfVoterMECE,how = 'left',left_on='ncid',right_on='ncid')
#Update records with no voting history as MECE = 5
gdfVoter.loc[gdfVoter.MECE.isnull(),"MECE"] = 5


#%% PART 2. CENSUS DATA
#Get the Census API key
print('2a. Getting the census API key')
censusKey = open("APIkey.txt","r").readline()

#Get the Census block features and attributes for the county
print('2b. Fetching/reading census block features')
if os.path.exists(block_shapefile_filename):
    print("2c. Reading block features from {}".format(block_shapefile_filename))
    gdfBlocks = gpd.read_file(block_shapefile_filename)
else:
    print("2c. Assembling block features from web resources...")
    gdfBlocks = get_block_features(state_fips,county_fips,block_shapefile_filename,censusKey)
#Add field for number of black households
gdfBlocks['BlackHH'] = gdfBlocks.HOUSING10 * gdfBlocks.PctBlack / 100

#Join blocks to voter points
print('2d. Joining block data to voter features')
gdfVoter = gpd.sjoin(gdfVoter,gdfBlocks,how='left',op='within')

#Save the to file containing voter point features that include voter history
# and data on the block in which the voter is located.
outFile = '{}\\VoterFeatures.shp'.format(COUNTY_folder)
print('2e. Saving output to {}. [Be patient...]'.format(outFile))
gdfVoter.to_file(outFile,format='shapefile')

#%% STEP 3. ASSIGN VOTER TURF VALUES TO VOTING POINTS
# Organizational units are areas managed by one or two 'super voters'.
# These areas should:
#  (1) have need, defined as having a certian number of black voters
#  (2) have leaders, defined as having two "MECE 1" voters
#  (3) be of manageable size, defined as fewer than 100 households
#  (4) 

# Subset Black voters in blocks with > 50% black
mask_Voter = gdfVoter['race_code'] == 'B'
mask_Block = gdfVoter['PctBlack'] >= 50
gdfVoter2 = gdfVoter.loc[mask_Voter & mask_Block]
outFile = '{}\\SubsetVoterFeatures.shp'.format(COUNTY_folder)
gdfVoter2.to_file(outFile,format='shapefile')

# Tally counts by MECE score in each block
#List each block and the number of voters in each MECE
dfMECE = (gdfVoter2.pivot_table(index='BLOCKID10',
                                 columns='MECE',
                                 aggfunc={'ncid':'count'})
           .fillna(0)
           .droplevel(0,axis=1)
           .reset_index())
dfMECE.columns = ['BLOCKID10','MECE1','MECE2','MECE3','MECE4','MECE5']
#Compute total voters in the block
dfMECE['Total']=dfMECE.iloc[:,0:5].sum(axis=1)
#Add block group attribute
dfMECE['BlkGrp']=dfMECE['BLOCKID10'].apply(lambda x: x[:-4])

#Remove blocks with fewer than 50 black households


# Join MECE counts to block features
gdfBlocks2 = pd.merge(gdfBlocks,dfMECE,on='BLOCKID10',how='inner')

# Subset blocks with at least two MECE 1 voters
gdfBlocks3 = gdfBlocks2.loc[gdfBlocks2["MECE1"]>=2,:]
gdfBlocks.shape

# Compute number of black households
#gdfBlocks3['BlackHH'] = gdfBlocks3.HOUSING10 * gdfBlocks3.PctBlack / 100
gdfBlocks4 = gdfBlocks3[gdfBlocks3.BlackHH >= 50]
gdfBlocks4.shape

# Subset blocks with > 50 black households
gdfBlocks2.to_file('{}\\BlockMece2.shp'.format(COUNTY_folder)) 
#%% Set Org Units

#Subset blocks with fewer than 50 black households
fcBlocksSubset  = gdfBlocks2[(gdfBlocks2.BlackHH < 50) & (gdfBlocks2.BlackHH > 10)].reset_index()
#Dissolve adjacent blocks
fcClusters = gpd.GeoDataFrame(geometry = list(fcBlocksSubset.unary_union))
#Add a static ID field to the geodataframe
fcClusters['ID'] = fcClusters.index
#Copy over crs to new file
fcClusters.crs = fcBlocksSubset.crs

#Spatially join the dissolved ID to the subset blocks layer
fcBlockSubset2 = gpd.sjoin(fcBlocksSubset,fcClusters,how='left',op='within').drop("index_right",axis=1)
#Initialize the field to contain new organizational unit IDs
fcBlockSubset2['OrgID'] = 0

#Spatially join the dissolved ID to the subset blocks layer
fcBlockSubset2 = gpd.sjoin(fcBlocksSubset,fcClusters,how='left',op='within').drop("index_right",axis=1)
#Initialize the field to contain new organizational unit IDs
fcBlockSubset2['OrgID'] = 0

#Compute total BHH for the dissolved blocks and add as attribute to bloc
sumHH = fcBlockSubset2.groupby('ID').agg({'BlackHH':'sum'})
#Join total aggregate BHH to the cluster geoframe
fcClusters2=pd.merge(fcClusters,sumHH,left_index=True,right_index=True)

#Save clusters with between 50 and 100 HH to a a new geoframe
fcClusters_keep1 = fcClusters2[(fcClusters2.BlackHH > 50) & (fcClusters2.BlackHH <= 100)].reset_index()

#Find IDs of dissolved blocks with HH > 100
fcTooBig = fcClusters2.query('BlackHH > 100')

#Iterate through each cluster (that has more than 100 BHH)
for idx in fcTooBig.ID:
    #Get the cluster ID
    clusterID = fcTooBig.loc[idx,"ID"]
    #Get the blocks with that ID
    fcCBlocks = fcBlockSubset2[(fcBlockSubset2.ID == clusterID) & (fcBlockSubset2.OrgID == 0)].reset_index()
    
    #Add the X coordinate as a column
    fcCBlocks['X'] = fcCBlocks.geometry.centroid.x
    
    #Start with the feat with the min X
    fcNbrs = fcCBlocks[fcCBlocks.X == fcCBlocks.X.max()]
    
    #Get the number of aggregate BGG in the selection
    BHH = fcNbrs.BlackHH.sum()
    geom = fcNbrs.geometry.unary_union

    #Increase the selection 
    iterX = 0 #Catch to avoid infinite loops
    while BHH < 100 and iterX < 100: 
        #Find the blocks that touch
        fcNbrs = fcBlockSubset2[(fcBlockSubset2.intersects(geom)) & #Select blocks that are adjacent
                                (fcBlockSubset2.BlackHH < 50) &     #...that have fewer than 50 BHH
                                (fcBlockSubset2.BlackHH > 10)       #...but have at least 10 BHH
                               ]
        BHH = fcNbrs.BlackHH.sum()
        geom = fcNbrs.geometry.unary_union
        iterX += 1

    #Select blocks and assign its OrgID
    fcBlockSubset2.loc[fcBlockSubset2.intersects(geom),'OrgID'] = idx+1000

#Select clusters that have been identified above and dissolve them, summing the BHHs
fcClusters_keep2 = fcBlockSubset2.query('OrgID > 1000').dissolve(by='OrgID',aggfunc={'BlackHH':'sum'})
#Update the index field
fcClusters_keep2['ID']=fcClusters_keep2.index
#Append to the clusters that already have between 50 and 100 BHH
fcAll1 = fcClusters_keep1.append(fcClusters_keep2,ignore_index=True,sort=False).reset_index()
#Append to original blocks (with more than 50 BHH)
fcOrig = gdfBlocks2.loc[gdfBlocks2['BlackHH']>50,['BlackHH','geometry']]
fcOrig['ID'] = fcOrig.index
fcAll2 = fcOrig.append([fcClusters_keep1,fcClusters_keep2],ignore_index=True,sort=False).reset_index()
fcAll2.to_file(os.path.join(COUNTY_folder,'OrgUnits.shp'))
