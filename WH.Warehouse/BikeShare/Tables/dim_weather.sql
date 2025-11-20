CREATE TABLE [BikeShare].[dim_weather] (

	[WeatherKey] bigint IDENTITY NOT NULL, 
	[DateKey] int NOT NULL, 
	[TimeKey] int NOT NULL, 
	[Temperature] float NULL, 
	[Humidity] int NULL, 
	[Precipitation] float NULL, 
	[WeatherCode] int NULL, 
	[WeatherCondition] varchar(50) NULL, 
	[WindSpeed] float NULL, 
	[FeelsLikeTemp] float NULL
);