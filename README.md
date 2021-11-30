# cricket-data-extractor-service

Contains extractors for 2 sources, Pulse Live (The official match data provider for bcci) and Cricsheet (an open source database maintained at cricsheet.org).

## Steps for extracting Cricsheet data. 

1. Go to cricsheet.org and download the zip for json matches that you want. 
2. Extract the zip file and update the unzipped directory address in the Makefile.
3. Create a local/server based MySQL database and add the connection string along with DB name.
4. Update the Makefile with SQL_CONNECTION_STRING variable.
5. Description of required Makefile variables:
  - FILESAVE_DIRECTORY: Directory to save a csv for processed data.
  - CRICSHEET_IPL_FILES_DIRECTORY: Direcrtory address of extracted cricsheet json folder for IPL.
  - CRICSHEET_T20I_FILES_DIRECTORY: Direcrtory address of extracted cricsheet json folder for T20Is.
6. Run make command ("make cricsheet_ipl" for ipl extraction and "make cricsheet_t20is" for t20is extraction).

## Steps for extracting Pulse Live (Hawk-eye) data.
