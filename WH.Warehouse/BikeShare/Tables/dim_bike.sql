CREATE TABLE [BikeShare].[dim_bike] (

	[BikeKey] bigint IDENTITY NOT NULL, 
	[RideableType] varchar(50) NOT NULL, 
	[BikeCategory] varchar(20) NULL
);