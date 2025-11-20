CREATE TABLE [BikeShare].[dim_station] (

	[StationKey] bigint IDENTITY NOT NULL, 
	[StationID] varchar(50) NOT NULL, 
	[StationName] varchar(255) NOT NULL, 
	[Latitude] float NULL, 
	[Longitude] float NULL, 
	[Area] varchar(100) NULL
);