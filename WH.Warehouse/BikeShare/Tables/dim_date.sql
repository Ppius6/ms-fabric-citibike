CREATE TABLE [BikeShare].[dim_date] (

	[DateKey] int NULL, 
	[Date] date NOT NULL, 
	[Year] int NULL, 
	[Quarter] int NULL, 
	[Month] int NULL, 
	[MonthName] varchar(20) NULL, 
	[DayOfMonth] int NULL, 
	[DayOfWeek] int NULL, 
	[DayName] varchar(20) NULL, 
	[WeekOfYear] int NULL, 
	[IsWeekend] bit NULL, 
	[Season] varchar(20) NULL, 
	[HolidayName] varchar(50) NULL
);