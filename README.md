# WakeVoter

Analysis of NC census and elections data to identify areas of low minority voter turnout. 



## Environment

* Python 3.6.6
* Jupyter | Geopandas | [GeoPy](https://github.com/geopy/geopy) | [ipyLeaflet](https://github.com/jupyter-widgets/ipyleaflet) | [Dask](https://dask.org/) | requests



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

## Scripts

* `Extract-WakeCo-Blocks-To-File.ipynb` 
  * Queries the online NC statewide block data feature class to extract just blocks in Wake County (FIPS 183). Output is places in the `data/spatial` folder as `tabblock2010_37183_pophu.shp`.

* `IsolateBlocks.ipynb`
  * Reads in the county census block features (created above) and Census attributes (retrieved from AFF query). The identifies the blocks with > 50% black tenure and extracts those block features meeting that criteria, saving them to the `data/spatial` folder as `tabblock2010_37183_BlackGT50Pct.shp`
  * 

