CREATE TABLE [BikeShare].[fct_trips] (

	[TripKey] bigint IDENTITY NOT NULL, 
	[RideID] varchar(255) NOT NULL, 
	[StartDateKey] int NOT NULL, 
	[StartTimeKey] int NOT NULL, 
	[EndDateKey] int NOT NULL, 
	[EndTimeKey] int NOT NULL, 
	[StartStationKey] bigint NULL, 
	[EndStationKey] bigint NULL, 
	[BikeKey] bigint NOT NULL, 
	[MemberKey] bigint NOT NULL, 
	[WeatherKey] bigint NULL, 
	[TripDurationMinutes] int NULL, 
	[TripDistanceKm] float NULL, 
	[StartLatitude] float NULL, 
	[StartLongitude] float NULL, 
	[EndLatitude] float NULL, 
	[EndLongitude] float NULL, 
	[SpeedKmh] float NULL, 
	[ProcessedAt] datetime2(6) NULL, 
	[ProcessingBatchID] varchar(50) NULL
);