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
import os, requests, zipfile, io, glob
from datetime import datetime

#Census API key
print('Getting the census API key')
censusKey = open("APIkey.txt","r").readline()
print("This product uses the Census Bureau Data API but ")
print("is not endorsed or certified by the Census Bureau.")

#%% RUN TIME VARIABLES

#Set data locations
NCSBE_folder ='.\\data\\NCSBE'     #Folder containing NC SBE data
CENSUS_folder = '.\\data\\Census'  #Folder containing Census data

#Get NC County data
data_url = 'https://transition.fcc.gov/form477/Geo/CensusBlockData/CSVFiles/North%20Carolina.zip'
dfCounties = (pd.read_csv(data_url,
                         usecols=('county','cnamelong'),
                         dtype='str').                   
            drop_duplicates(keep='first').
            reset_index())
            
#Create column of just county name (drop " county")
dfCounties['cname'] = dfCounties['cnamelong'].str.split(pat=' ',expand=True)[0] 

#Iterate for county
state_fips = '37'
for i,row in dfCounties.iterrows():
    county_fips = row['county']
    county_name = row['cname'].upper()
    if i == 2: break

print("Processing {} county".format(county_name.title()))

#Create a folder to hold county data
COUNTY_folder = '.\\data\\OUTPUT\\{}'.format(county_name)
if not(os.path.exists(COUNTY_folder)):
    os.mkdir(COUNTY_folder)

#Set the output filenames
fnVoterShapefile = os.path.join(COUNTY_folder,'{}_voter_points.shp'.format(county_name))
fnVoterShapefileSubset = os.path.join(COUNTY_folder,'{}_voter_subset_points.shp'.format(county_name))
fnBlockShapefile = os.path.join(COUNTY_folder,'{}_blocks.shp'.format(county_name))
fnOrgunitsShapefile = os.path.join(COUNTY_folder,'{}_orgunits.shp'.format(county_name))

#Clean up
del i, row, data_url, dfCounties

#%% FUNCTIONS
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
    print("     ...downloading data from {}".format(theURL))
    response = requests.get(theURL,params) 
    response_json = response.json()
    #Convert JSON to pandas dataframe
    print("     ...cleaning data...")
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

#%% 1a. Fetch statewide voter registration data
print("1a. Getting statewide voting registration data")

#See if file exists; if so skip the download
file_list = glob.glob(NCSBE_folder+'/**/ncvoter_Statewide.txt',recursive=True)
if len(file_list) > 0:
    state_voter_reg_file = file_list[0]
    print("   File already downloaded")
#Otherwise download
else:
    #Fetch and unzip the file
    print("   Retrieving registration file from NC SBE server [Be patient...]")
    fileURL = 'http://dl.ncsbe.gov/data/ncvoter_Statewide.zip'
    r = requests.get(fileURL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    print("    Unpacking data...")
    z.extractall(NCSBE_folder)
    #Get the file path
    state_voter_reg_file = glob.glob(NCSBE_folder+'/**/ncvoter_Statewide.txt',recursive=True)[0]
    print("    Voter registration data stored as\n  [{}]".format(state_voter_reg_file))
    #Clean up 
    del r,z

#Clean up
del file_list
      
#%% 1b. Fetch statewide votger history data
print("1b. Getting statewide voting history data")

#See if file exists; if so skip the download
file_list = glob.glob(NCSBE_folder+'/**/ncvhis_Statewide.txt',recursive=True)
if len(file_list) > 0:
    state_voter_history_file = file_list[0]
    print("   File already downloaded")
#Otherwise download
else:
    #Fetch and unzip the file
    print("   Retrieving address file from NC SBE server [Be patient...]")
    fileURL = 'http://dl.ncsbe.gov/data/ncvhis_Statewide.zip'
    r = requests.get(fileURL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    print("   Unpacking data...")
    z.extractall(NCSBE_folder)
    #Get the file path
    state_voter_history_file = glob.glob(NCSBE_folder+'/**/ncvhis_Statewide.txt',recursive=True)[0]
    print("   Voter history data stored as\n  [{}]".format(state_voter_history_file))
    #Clean up 
    del fileURL, r, z

#Clean up
del file_list

#%% 1c. Fetch statewide address data
print("1c. Getting statewide voting address data")

#See if file exists
file_list = glob.glob(NCSBE_folder+'/**/address_points_sboe.txt',recursive=True)
if len(file_list) > 0:
    state_address_file = file_list[0]
    print("   File already downloaded")

else: #Otherwise retrieve the file from the NC SBE server
    print("   Retrieving address file from NC SBE server [Be patient...]")
    fileURL = 'https://s3.amazonaws.com/dl.ncsbe.gov/ShapeFiles/address_points_sboe.zip'
    r = requests.get(fileURL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    print("   Unpacking data...")
    z.extractall(NCSBE_folder)
    #Get the file path
    state_address_file = glob.glob(NCSBE_folder+'/**/address_points_sboe.txt',recursive=True)[0]
    print("   Statewide data stored as\n  [{}]".format(state_address_file))
    #Clean up 
    del fileURL, r,z

#Clean up
del file_list

#Get associated metadata file
state_address_metadata = glob.glob(NCSBE_folder+'/**/address_points_data_format.txt',recursive=True)[0] 

#%% 1d. Subset voting address file for county
print("1d. Subsetting voting address data for {} county".format(county_name.title()))

#See if the county file already exists, if so create and return a dataframe
file_list = glob.glob(NCSBE_folder+'/**/address_points_{}.csv'.format(county_name),recursive=True)
if len(file_list) > 0:
    county_address_file = file_list[0]
    print("   Found county address file:\n  [{}]".format(county_address_file))
    dfCountyAddresses = pd.read_csv(county_address_file,dtype='str')
    dfCountyAddresses['latitude'] = dfCountyAddresses['latitude'].astype('float')
    dfCountyAddresses['longitude'] = dfCountyAddresses['longitude'].astype('float')
    
else: #Subset county data from state data
    print("    Building country address file...")
    #See whether state data dataframe is in memory
    if not 'dfStateAddresses' in dir():
        #Otherwise read in the state data, using the metadata to supply column names
        print("    ...Generating statewide address dataframe...")
        #Generate a list of columns from the metadata file 
        with open(state_address_metadata,'r') as colFile:
            theText = colFile.readlines()
            columns = [theLine.split("\n")[0].split()[0] for theLine in theText[7:]]
        dfStateAddresses = pd.read_csv(state_address_file,sep='\t',dtype='str',
                                       header=None,index_col=0,names = columns)
        dfStateAddresses['latitude'] = dfStateAddresses['latitude'].astype('float')
        dfStateAddresses['longitude'] = dfStateAddresses['longitude'].astype('float')
        print("   ...{} statewide records loaded".format(dfStateAddresses.shape[0]))
        #Clean up
        del columns, theText
        
    #Extract county addresses and save
    dfCountyAddresses = dfStateAddresses[dfStateAddresses.county == county_name]
    print("   ...{} county records extracted".format(dfCountyAddresses.shape[0]))
    #Save to a file
    county_address_file = state_address_file.replace('sboe.txt','{}.csv'.format(county_name))
    dfCountyAddresses.to_csv(county_address_file,index=False)
    print("    County address file created:\n  [{}]".format(county_address_file))

#Clean up
del file_list

#%% 1e. Compute county MECE scores
print("1e. Computing MECE scores for {} voters".format(county_name))  
#Read the data into a dataframe (if not done already)
if not 'dfStateHistory' in (dir()):
    print("    Reading in state voting history file. [Be patient]")
    dfStateHistory = pd.read_csv(state_voter_history_file,sep='\t',
                                 usecols=('county_desc','election_lbl','ncid'))

#Subset records for the county
print("   Subseting county voting history records")
dfCountyHistory = dfStateHistory[dfStateHistory['county_desc']==county_name]

#Subset records for the elections of interest
print("   Subsetting election data")
elections = ('10/10/2017','11/07/2017','11/06/2018','11/08/2016','11/06/2012')
dfSubset = dfCountyHistory.loc[dfCountyHistory.election_lbl.isin(elections),:]

#Pivot on these elections
print("   Determining MECE scores",end="")
dfVoterMECE = pd.pivot_table(dfSubset,
                             columns = 'election_lbl',
                             index = 'ncid',
                             aggfunc = 'count',
                             fill_value = 0
                             ).droplevel(level=0,axis=1)

#Rename columns
print(" .",end="")
dfVoterMECE.rename({'10/10/2017':'Oct17','11/06/2012':'Nov12',
                    '11/06/2018':'Nov18','11/07/2017':'Nov17',
                    '11/08/2016':'Nov16'}, axis=1,inplace=True)

#Create filters
print(" .",end="")
e12 = dfVoterMECE.Nov12 == 1
e16 = dfVoterMECE.Nov16 == 1
e17 = (dfVoterMECE.Oct17 == 1) | (dfVoterMECE.Nov17 == 1)
e18 = dfVoterMECE.Nov18 == 1

#Apply filters to assign MECE values
print(" .")
dfVoterMECE.loc[e17, "MECE"] = 1
dfVoterMECE.loc[~e17 & e18, "MECE"] = 2
dfVoterMECE.loc[~e17 & ~e18 & e16, "MECE"] = 3
dfVoterMECE.loc[~e17 & ~e18 & ~e16 & e12, "MECE"] = 4
dfVoterMECE.loc[~e17 & ~e18 & ~e16 & ~e12, "MECE"] = 5

#Clean up 
del e12,e16,e17,e18, elections, dfSubset

#%% 1f. Convert registgration data to spatial features
print("1f. Converting {} voter data to spatial features".format(county_name.title()))

#Read state registration data into dataframe, if not done already
if not "dfStateRegistration" in dir():
    print("    Reading in the voter registration data file [be patient]...")
    dfStateRegistration = pd.read_csv(state_voter_reg_file,
                                      usecols=['county_desc','voter_reg_num','res_street_address',
                                               'res_city_desc','state_cd','zip_code','precinct_abbrv',
                                               'race_code','ethnic_code','gender_code','ncid',
                                               'mail_addr1','mail_city','mail_state','mail_zipcode',
                                               'full_phone_number','birth_age','voter_reg_num',
                                               'last_name','first_name','middle_name','precinct_abbrv'],
                                               sep='\t',
                                               dtype='str',
                                               encoding = "ISO-8859-1",low_memory=False)

#Select records for the provided county name - into a new dataframe
print("    Selecting records for {} county...".format(county_name),end='')
dfCountyRegistration = dfStateRegistration[dfStateRegistration['county_desc'] == county_name.upper()].reindex()
print(" {} records selected".format(dfCountyRegistration.shape[0]))


#Drop the county name from the table and set the voter registration # as index
print("    Tidying data...")
dfCountyRegistration.drop('county_desc',axis=1,inplace=True)
dfCountyRegistration.set_index('voter_reg_num',inplace=True)
#Drop rows with no address data
dfCountyRegistration.dropna(how='any',inplace=True,
                subset=['res_street_address','res_city_desc','state_cd','zip_code'])
#Remove double spaces from the residential address field 
dfCountyRegistration['res_street_address'] = dfCountyRegistration['res_street_address'].apply(lambda x: ' '.join(x.split()))

#Join coords from address file to county registration data
print("    Joining coordinates to voter data")
dfCountyRegistration = pd.merge(left=dfCountyRegistration,
                                 left_on=['res_street_address','res_city_desc','zip_code'],
                                 right=dfCountyAddresses,
                                 right_on=['st_address','city','zip'],
                                 how='left')

#Drop records that weren't geocoded
dfCountyRegistration.dropna(axis=0,subset=['longitude','latitude'],inplace=True)

#%% 1g. Append MECE scores to county registration data
print("1g. Appending MECE scores to {} registration data".format(county_name.title()))

#Merge MECE scores to County Registration data
print("    Merging  MECE scores to voter dataframe")
dfCountyRegistration = pd.merge(dfCountyRegistration,
                                dfVoterMECE,
                                how = 'left',
                                left_on='ncid',
                                right_on='ncid')

#Update records with no voting history as MECE = 5
print("    Setting null MECE scores '5'")
dfCountyRegistration.loc[dfCountyRegistration.MECE.isnull(),"MECE"] = 5

#Convert to geodataframe
print("    Converting to spatial dataframe")
from shapely.geometry import Point
geom = [Point(x,y) for x,y in zip(dfCountyRegistration.longitude,dfCountyRegistration.latitude)]
gdfVoter = gpd.GeoDataFrame(dfCountyRegistration,geometry=geom,crs={'init':'epsg:4269'})
del geom

#%% DELETE??
'''
#Save the geodataframe, if a voter shapefile name is set
if fnVoterShapefile:
    print("   Saving to {} [Be patient...]".format(fnVoterShapefile))
    gdfVoter.to_file(fnVoterShapefile,filetype='Shapefile')
    
    #Write projection to .prj file
    with open(fnVoterShapefile[:-3]+'prj','w') as outPrj:
        outPrj.write('GEOGCS["GCS_North_American_1983",'+
                     'DATUM["D_North_American_1983",'+
                     'SPHEROID["GRS_1980",6378137.0,298.257222101]],'+
                     'PRIMEM["Greenwich",0.0],'+
                     'UNIT["Degree",0.0174532925199433]]')
    
    #Write metadata  to .txt file
    current_date = datetime.now().strftime("%Y-%m-%d")
    with open(fnVoterShapefile[:-3]+'txt','w') as outTxt:
        outTxt.write('Voter registration data for {} Co. extracted from\n'.format(county_name))
        outTxt.write('NC SBE: https://www.ncsbe.gov/data-stats/other-election-related-data\n')
        outTxt.write('File created on {}'.format(current_date))
'''
#%% 2a. Extract state block features to geodataframe
print("\n\nSTEP 2: CENSUS DATA PROCESSING")
print("2a. Extracting state block features into a geodataframe")

#See if the data have already been pulled; if so, read into dataframe and return
if os.path.exists(fnBlockShapefile):
    print("  Data already downloaded\n  [{}]".format(fnBlockShapefile))
    print("  ...reading data into a dataframe")
    gdfBlocks = gpd.read_file(fnBlockShapefile)

#Otherwise download and create the file
else:
    #Set the URL where the data are downloaded
    dataURL = 'https://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/tabblock2010_{}_pophu.zip'.format(state_fips)

    #Read the STATE block file into memory, if not already
    if not "fcStBlocks" in dir(): 
        #See if the statewide block shapefile has been downloaded
        stateCensusBlocksFile = './data/Census/StateBlocks.shp'
        if os.path.exists(stateCensusBlocksFile):
            print("    Creating dataframe from existing state block feature class.")
            gdfStBlocks = gpd.read_file(stateCensusBlocksFile)
        else: #Pull the state block data for the supplied FIPS code
            print("    Downloading blocks for state FIPS {}; this take a few minutes...".format(state_fips))
            gdfStBlocks = gpd.read_file(dataURL)
            print("    Saving state blocks to {}".format(stateCensusBlocksFile))
            gdfStBlocks.to_file(stateCensusBlocksFile)

    #Subset county blocks from state blocks
    print("    Subsetting data for County FIPS {} ".format(county_fips))
    gdfCoBlocks = gdfStBlocks[gdfStBlocks.COUNTYFP10 == county_fips]
    
    #Retrieve block attribute data
    print("    Fetching block attribute data")
    dfAttribs = _get_block_attributes(state_fips,county_fips,censusKey)
    gdfCoBlocks =  pd.merge(left=gdfCoBlocks,left_on='BLOCKID10',
                            right=dfAttribs,right_on='GEOID10',
                            how='outer')
    #Clean up
    del dfAttribs
    
    #Add field for number of black households
    print("    Calculating 'BlackHH' - # of black households in block")
    gdfCoBlocks['BlackHH'] = round(gdfCoBlocks.HOUSING10 * gdfCoBlocks.PctBlack / 100).astype('int')
    
    #Save to a shapefile, if one is given
    if fnBlockShapefile != '':
        print("    Saving to {}".format(fnBlockShapefile))
        gdfCoBlocks.to_file(fnBlockShapefile,filetype='Shapefile')
        
        #Write projection to .prj file
        with open(fnBlockShapefile[:-3]+'prj','w') as outPrj:
            outPrj.write('GEOGCS["GCS_North_American_1983",'+
                         'DATUM["D_North_American_1983",'+
                         'SPHEROID["GRS_1980",6378137.0,298.257222101]],'+
                         'PRIMEM["Greenwich",0.0],'+
                         'UNIT["Degree",0.0174532925199433]]')
        
        #Write metadata  to .txt file
        current_date = datetime.now().strftime("%Y-%m-%d")
        with open(fnBlockShapefile[:-3]+'txt','w') as outTxt:
            outTxt.write('Census block data for FIPS{}{} extracted from\n'.format(state_fips,county_fips) +
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
        
        #Clean up
        del current_date
        
    #Clean up
    del dataURL
        
#%% 2b. Join block number to voter features 
print("2b. Tagging voter data with block numbers")     
print("    Spatially joining block ID to voter features")
gdfVoter = gpd.sjoin(gdfVoter,gdfCoBlocks,how='left',op='within')
print("    ...removing extraneous columns")
gdfVoter.drop(columns=['index_right','STATEFP10', 'COUNTYFP10', 'TRACTCE10', 
                       'BLOCKCE', 'GEOID10', 'PARTFLG'],axis='columns',inplace=True)

#Save dataframe to shapefile, if one is given
if fnVoterShapefile:
    #Save to shapfile, if extension is ".shp"
    if fnVoterShapefile[-4:] == '.shp':
        print("    Saving geodataframe as shapefile. [Be patient...]")
        gdfVoter.to_file(fnVoterShapefile,format='shapefile')
    else: #Otherwise, save to csv
        print("  Saving geodataframe as CSV file...")
        gdfVoter.to_csv(fnVoterShapefile,index_label="OID")
    
    #Write metadata  to .txt file
    print('    ...writing metadata.')
    current_date = datetime.now().strftime("%Y-%m-%d")
    with open(fnVoterShapefile[:-3]+'txt','w') as outTxt:
        outTxt.write('Voter registration and history data for {} Co. extracted from\n'.format(county_name))
        outTxt.write('NC SBE: https://www.ncsbe.gov/data-stats/other-election-related-data\n')
        outTxt.write('Census block data appended to points.\n\n')
        outTxt.write('File created on {}'.format(current_date))
        
#%%2c. Extract black voters in majority black blocks
print('2c. Extracting black voters in majority black blocks')   
#def subset_voter_points(gdf_voters,output_shapefile=''):
mask_Voter = gdfVoter['race_code'] == 'B'
mask_Block = gdfVoter['PctBlack'] >= 50
gdfBlackVoter = gdfVoter.loc[mask_Voter & mask_Block]

#HALT IF NO MAJORITY BLACK BLOCKS
if gdfBlackVoter.shape[0] == 0:
    print("{0}\nNO MAJORTY BLACK BLOCKS IN {1} COUNTY. EXITING\n{0}".format(20*"-",county_name.upper()))
    import sys
    sys.exit(0)
if fnVoterShapefileSubset:
    #Save to shapfile, if extension is ".shp"
    if fnVoterShapefileSubset[-4:] == '.shp':
        print("   ...Saving geodataframe as shapefile...")
        gdfBlackVoter.to_file(fnVoterShapefileSubset,format='shapefile')
    else: #Otherwise, save to csv
        print("   ...Saving geodataframe as CSV file...")
        gdfBlackVoter.to_csv(fnVoterShapefileSubset,index_label="OID")
        
    #Write metadata  to .txt file
    print('    ...writing metadata.')
    current_date = datetime.now().strftime("%Y-%m-%d")
    with open(fnVoterShapefileSubset[:-3]+'txt','w') as outTxt:
        outTxt.write('Subset of voter points\n'.format(county_name))
        outTxt.write('Voting points for only black voter in majority black blocks\n\n')
        outTxt.write('File created on {}'.format(current_date))
#%% 2d. Tally Block MECE scores and append to Block features
print('2d. Tally Block MECE scores')
#Ensure gdfVoter has the right columns
if not ("MECE" in gdfBlackVoter.columns):
    print(' ERROR: [MECE] not found in supplied dataframe.')

#Pivot the voter data on MECE scores, tallying counts
print("    Pivoting data on MECE values")
dfBlockMECE = (gdfBlackVoter.pivot_table(index='BLOCKID10',
                                columns='MECE',
                                aggfunc={'ncid':'count'})
              .fillna(0)               # Set NaNs to zero
              .droplevel(0,axis=1)     # Drop multi-index
              .reset_index())          # Reset row index
#Subset columns
print("    Removing extraneous columns")
#dfBlockMECE.columns = ['BLOCKID10','MECE1','MECE2','MECE3','MECE4','MECE5']
dfBlockMECE.rename(columns={1.0:'MECE1',2.0:'MECE2',3.0:'MECE3',4.0:'MECE4',5.0:'MECE5'},
                   inplace=True)
#Compute total voters in the block
print("    Computing total election count per voter")
dfBlockMECE['Total']=dfBlockMECE.sum(axis=1)
#Convert dtypes to integers
colList = dfBlockMECE.columns[1:]
dfBlockMECE[colList] = dfBlockMECE[colList].astype('int')

#%% 3. PROCESS ORG UNITS (TURFS)
print("STEP 3 - PROCESSING ORG UNITS (TURFS)")

#--- Step 3a. Select blocks that are majority black and add MECE count data
print(" 3a. Subsetting blocks that are majority black.")
gdfMajBlackBlocks = gdfCoBlocks.query('PctBlack >= 50')

#--- Step 3b. Join MECE data (and tidy up fields)
print(" 3b. Joining block MECE data to selected blocks.")
gdfMajBlackBlocks = pd.merge(gdfMajBlackBlocks,dfBlockMECE,on='BLOCKID10',how='left').fillna(0)
# Fix dtypes (Pandas defaults back to floats)
gdfMajBlackBlocks[gdfMajBlackBlocks.columns[-6:]] = gdfMajBlackBlocks[gdfMajBlackBlocks.columns[-6:]] .astype('int')
gdfMajBlackBlocks.drop(['STATEFP10','COUNTYFP10','TRACTCE10','BLOCKCE','PARTFLG'],
                 axis=1,inplace=True)

#--- Step 3c. Subset majority black blocks with > 50 black HH and save as gdf_Org1 
#  to be merged with other org units later.
print(" 3c. Keeping majority black blocks with > 50 black households to 'Org1'")
gdf_Org1 = gdfMajBlackBlocks.query('BlackHH > 50').reset_index()
gdf_Org1.drop(['index', 'BLOCKID10','GEOID10'],axis=1,inplace=True)
gdf_Org1['OrgID'] = gdf_Org1.index + 1
gdf_Org1['OrgType'] = 'block'

#%%
#--- Step 3d. Select the majority black blocks with fewer than 50 black HH for clustering
print("3d. Clustering the remaining blocks...")
#Isolate blocks to be clustered, i.e., blocks with > 50 BHH
gdfMajBlackBlocks_LT50 = gdfMajBlackBlocks.query('BlackHH < 50')


#Step 3d1. Cluster adjacent blocks into a single feature and assing a ClusterID
print("3d(1). Creating intitial clusters...")
gdfClusters = gpd.GeoDataFrame(geometry = list(gdfMajBlackBlocks_LT50.unary_union))
gdfClusters['ClusterID'] = gdfClusters.index
gdfClusters.crs = gdfMajBlackBlocks_LT50.crs #Set the coordinate reference system


#Step 3d2. Recalculate population stats for the clusters
print("3d(2). Computing number of black households in new clusters...")
# -> Done by first spatially joininig the cluster ID to the blocks w/ < 50 Black HH
gdfMajBlack_LT50_2 = gpd.sjoin(gdfMajBlackBlocks_LT50,gdfClusters,
                               how='left',op='within').drop("index_right",axis=1)
# -> Next we dissolve on the cluster ID computing SUM of the numeric attributes
#    and updating the percentage fields
gdfClusters_2 = gdfMajBlack_LT50_2.dissolve(by='ClusterID', aggfunc='sum')
gdfClusters_2['PctBlack'] = gdfClusters_2['P003003'] / gdfClusters_2['P003001'] * 100
gdfClusters_2['PctBlack18'] = gdfClusters_2['P010004'] / gdfClusters_2['P010001'] * 100


#Step 3d3. Remove block clusters with fewer than 50 BHH; these are impractical
print("3d(3). Removing clusters still with < 50 black households (impractical)...")
gdfClusters_2 = gdfClusters_2.query('BlackHH >= 50')

#Step 3d4. Select clusters with fewer than 100 BHH and save as gdf_Org2, to be merged...
print("3d(4). Keeping new clusters with fewer than 100 black households: 'Org2'")
gdf_Org2 = gdfClusters_2.query('BlackHH <= 100').reset_index()
gdf_Org2['OrgID'] = gdf_Org1['OrgID'].max() + gdf_Org2.index + 1
gdf_Org2['OrgType'] = 'block aggregate'

#Step 3d5. For clusters that are too big (> 100 Black HH), cluster incrementally
#  so that clusters have up to 100 Black HH. These will be saved as gdf_Org3
print("3d(5).. Reclustering clusters with > 100 HH into smaller aggregates...")
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
gdfAllOrgs_out.to_file(fnOrgunitsShapefile)
gdfAllOrgs_out.drop(['geometry'],axis=1).to_csv(fnOrgunitsShapefile[:-3]+'csv',index=False)

##Write metdatada
with open(fnOrgunitsShapefile[:-4]+"README.txt",'w') as meta:
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
print('    Org units saved to {}'.format(fnOrgunitsShapefile))

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
gdfVoter_out.to_file(fnVoterShapefile)
gdfVoter_out.drop(['geometry'],axis=1).to_csv(fnVoterShapefile[:-3]+'csv',index=False)

##Write metdatada
with open(fnVoterShapefile[:-4]+"README.txt",'w') as meta:
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
        
print('    Voter data saved to {}'.format(fnVoterShapefile))