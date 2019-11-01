# WakeVoter

Analysis of NC census and elections data to identify areas of low minority voter turnout. 



## Environment

* Python 3.6.6
* Jupyter | Geopandas | [GeoPy](https://github.com/geopy/geopy) | requests | spyder | [folium](http://python-visualization.github.io/folium/) 

#### Local Instalation

* Clone the repo `$ git clone https://github.com/14-cities/WakeVoter.git`
* Install requirements with `$ pip install -r reqs.txt` (**NOTE:** it is heavily encouraged to do this in a virtual environment)
* Request an (API key)[https://api.census.gov/data/key_signup.html] from  the US Census
* When the key arrives in an email, copy and paste it into a file called `APIkey.txt` at the root of WakeVoter
* To run the applicaiton, start a new Python shell session and import the `VoteSqaud.py` file
  * `$ python`
  * `>>> import VoteSquad.py`


#### OS Specific troubleshooting

* On **Mac OS X**, if you get the error `OSError: Could not find libspatialindex_c library file` you may need to run `$ brew install spatialindex` to get rtree working properly. See this [GitHub issue](https://github.com/gboeing/osmnx/issues/45)



## Data

#### Voting registration and history data

* https://www.ncsbe.gov/data-stats/other-election-related-data 
  * Voter Registration Data By County > 
    * [Statewide Voter Registration](http://dl.ncsbe.gov/data/ncvoter_Statewide.zip)
    *  [Statewide Voter History](http://dl.ncsbe.gov/data/ncvhis_Statewide.zip)

#### Voting precinct shape files 

* <https://dl.ncsbe.gov/index.html?prefix=PrecinctMaps/>
  * [SBE_PRECINCTS_20190507.zip](https://s3.amazonaws.com/dl.ncsbe.gov/PrecinctMaps/SBE_PRECINCTS_20190507.zip)  

#### 2010 Census Blocks 

* [Block features with population & housing data](https://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/tabblock2010_37_pophu.zip)
* Demographic data
  * https://factfinder.census.gov/faces/nav/jsf/pages/searchresults.xhtml?refresh=t
  * Geographies: 
    * All geographic types
    * Geographic type: `.... .... .... Block - 100`
    * Geographic vintage: `2010`
    * State: `North Carolina`
    * County: `Wake`
    * Census tract: 
    * Select one ore more
  * Topics: 
    * P1: Race
    * P10: Race for population 18 and over

Address points

* https://s3.amazonaws.com/dl.ncsbe.gov/ShapeFiles/list.html
* https://dl.ncsbe.gov/index.html?prefix=ShapeFiles/
* https://s3.amazonaws.com/dl.ncsbe.gov/ShapeFiles/address_points_sboe.zip

---

## Workflows

### 1. Extract and organize voting data for [Wake] county

* Pull the voter data from the NC SBE servers and subset for the selected county:
  * Manually download the [Statewide Voter Registration](http://dl.ncsbe.gov/data/ncvoter_Statewide.zip) and [Statewide Voter History](http://dl.ncsbe.gov/data/ncvhis_Statewide.zip) data sets to a local folder.
    * _Will be replaces with code to download files from NCSBE servers..._
* Run `get_county_voter_registration_file` to retrieve/build a county level registration csv file.
  * Run `get_county_voter_history_file` to retrieve/build a county level registration csv file from the state file.
  * Run `get_address_data()` to pull [NC Address Points](https://s3.amazonaws.com/dl.ncsbe.gov/ShapeFiles/address_points_sboe.zip) data file to a local folder.
  
* Assemble a feature class of voting points for the county
  * Run `get_voter_data()` to select voting records for a specific county and assemble them into a shapefile.
    * This uses the address file to attach coordinates to voter registration data
  * 

### 1. Identify census block groups with > 50% black voters

* Obtain block feature dataset and attribute dataset, subset for Wake Co.
* Join attributes to features and isolate block features with > 50 black voters
* Tag block features with voting precincts

### 2. Compute voting frequency data and geocode it

* Extract voting registration and history data for Wake Co. 
* Geocode voter registration data
* Tally the number of elections voted in for each voter registration & join to geocode data

### 3. Select voting data falling within selected census blocks

* Spatially join voting registration data with census blocks & precinct information
* Compile list of super voters within each block (voters in all elections)
* Compile list of slacker voters within each block
* Identify contiguous blocks, within precincts, and tally slacker voters 
* Assign super voters for each 100 slacker voters in contiguous blocks.

---