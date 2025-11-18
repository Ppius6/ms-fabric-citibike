CREATE TABLE [dbo].[fact_bike_trips] (

	[trip_id] bigint NOT NULL, 
	[date_key] int NULL, 
	[start_time_key] int NULL, 
	[end_time_key] int NULL, 
	[start_station_id] varchar(20) NULL, 
	[end_station_id] varchar(20) NULL, 
	[bike_type] varchar(20) NULL, 
	[member_type] varchar(10) NULL, 
	[trip_duration_minutes] int NULL, 
	[estimated_distance_km] float NULL
);