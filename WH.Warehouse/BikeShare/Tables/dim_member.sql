CREATE TABLE [BikeShare].[dim_member] (

	[MemberKey] bigint IDENTITY NOT NULL, 
	[MemberType] varchar(20) NOT NULL, 
	[CustomerSegment] varchar(20) NULL
);