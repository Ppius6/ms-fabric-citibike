CREATE TABLE [BikeShare].[dim_time] (

	[TimeKey] int NULL, 
	[Hour] int NOT NULL, 
	[HourLabel] varchar(20) NULL, 
	[TimeOfDay] varchar(20) NULL, 
	[IsBusinessHour] bit NULL, 
	[IsPeakHour] bit NULL
);