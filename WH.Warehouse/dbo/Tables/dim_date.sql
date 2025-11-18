CREATE TABLE [dbo].[dim_date] (

	[date_key] int NULL, 
	[full_date] date NULL, 
	[day] int NULL, 
	[month] int NULL, 
	[year] int NULL, 
	[quarter] int NULL, 
	[day_name] varchar(10) NULL, 
	[month_name] varchar(10) NULL, 
	[is_weekend] bit NULL, 
	[week_number] int NULL
);