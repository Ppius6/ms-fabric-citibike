CREATE   PROCEDURE BikeShare.LoadTripData (@LoadMonth INT, @LoadYear INT)
AS
BEGIN
    -- Populate Date Dimension
    INSERT INTO BikeShare.dim_date (
        DateKey, Date, Year, Quarter, Month, MonthName, DayOfMonth, 
        DayOfWeek, DayName, WeekOfYear, IsWeekend, Season, HolidayName
    )
    SELECT DISTINCT
        CONVERT(INT, FORMAT(CAST(started_at AS DATE), 'yyyyMMdd')) as DateKey,
        CAST(started_at AS DATE) as Date,
        YEAR(CAST(started_at AS DATE)) as Year,
        DATEPART(QUARTER, CAST(started_at AS DATE)) as Quarter,
        MONTH(CAST(started_at AS DATE)) as Month,
        DATENAME(MONTH, CAST(started_at AS DATE)) as MonthName,
        DAY(CAST(started_at AS DATE)) as DayOfMonth,
        DATEPART(WEEKDAY, CAST(started_at AS DATE)) as DayOfWeek,
        DATENAME(WEEKDAY, CAST(started_at AS DATE)) as DayName,
        DATEPART(WEEK, CAST(started_at AS DATE)) as WeekOfYear,
        CASE WHEN DATEPART(WEEKDAY, CAST(started_at AS DATE)) IN (1,7) THEN 1 ELSE 0 END as IsWeekend,
        CASE 
            WHEN MONTH(CAST(started_at AS DATE)) IN (12,1,2) THEN 'Winter'
            WHEN MONTH(CAST(started_at AS DATE)) IN (3,4,5) THEN 'Spring'
            WHEN MONTH(CAST(started_at AS DATE)) IN (6,7,8) THEN 'Summer'
            ELSE 'Fall'
        END as Season,
        NULL as HolidayName
    FROM BikeShare.staging_citibiketrips
    WHERE YEAR(CAST(started_at AS DATE)) = @LoadYear 
        AND MONTH(CAST(started_at AS DATE)) = @LoadMonth
        AND NOT EXISTS (SELECT 1 FROM BikeShare.dim_date WHERE dim_date.DateKey = CONVERT(INT, FORMAT(CAST(started_at AS DATE), 'yyyyMMdd')));

    -- Populate Time Dimension (for all possible hours)
    INSERT INTO BikeShare.dim_time (TimeKey, Hour, HourLabel, TimeOfDay, IsBusinessHour, IsPeakHour)
    SELECT 
        h.Hour as TimeKey,
        h.Hour as Hour,
        CONCAT(RIGHT('0' + CAST(h.Hour AS VARCHAR(2)), 2), ':00-', RIGHT('0' + CAST((h.Hour + 1) % 24 AS VARCHAR(2)), 2), ':00') as HourLabel,
        CASE 
            WHEN h.Hour BETWEEN 5 AND 11 THEN 'Morning'
            WHEN h.Hour BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN h.Hour BETWEEN 17 AND 21 THEN 'Evening'
            ELSE 'Night'
        END as TimeOfDay,
        CASE WHEN h.Hour BETWEEN 8 AND 17 THEN 1 ELSE 0 END as IsBusinessHour,
        CASE WHEN h.Hour IN (7,8,9,17,18,19) THEN 1 ELSE 0 END as IsPeakHour
    FROM (SELECT number as Hour FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),
          (13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23)) AS Hours(number)) h
    WHERE NOT EXISTS (SELECT 1 FROM BikeShare.dim_time WHERE dim_time.TimeKey = h.Hour);

    -- Populate Station Dimension
    INSERT INTO BikeShare.dim_station (StationID, StationName, Latitude, Longitude, Area)
    SELECT DISTINCT 
        start_station_id,
        start_station_name,
        start_lat,
        start_lng,
        CASE 
            WHEN start_station_name LIKE '%JC%' THEN 'Jersey City'
            WHEN start_station_name LIKE '%HB%' THEN 'Hoboken'
            ELSE 'Other'
        END as Area
    FROM BikeShare.staging_citibiketrips
    WHERE start_station_id IS NOT NULL 
        AND start_station_name IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM BikeShare.dim_station WHERE dim_station.StationID = start_station_id)
    
    UNION
    
    SELECT DISTINCT 
        end_station_id,
        end_station_name,
        end_lat,
        end_lng,
        CASE 
            WHEN end_station_name LIKE '%JC%' THEN 'Jersey City'
            WHEN end_station_name LIKE '%HB%' THEN 'Hoboken'
            ELSE 'Other'
        END as Area
    FROM BikeShare.staging_citibiketrips
    WHERE end_station_id IS NOT NULL 
        AND end_station_name IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM BikeShare.dim_station WHERE dim_station.StationID = end_station_id);

    -- Populate Bike Dimension
    INSERT INTO BikeShare.dim_bike (RideableType, BikeCategory)
    SELECT DISTINCT 
        rideable_type,
        CASE 
            WHEN rideable_type LIKE '%electric%' THEN 'electric'
            ELSE 'classic'
        END as BikeCategory
    FROM BikeShare.staging_citibiketrips
    WHERE NOT EXISTS (SELECT 1 FROM BikeShare.dim_bike WHERE dim_bike.RideableType = rideable_type);

    -- Populate Member Dimension
    INSERT INTO BikeShare.dim_member (MemberType, CustomerSegment)
    SELECT DISTINCT 
        member_casual,
        CASE 
            WHEN member_casual = 'member' THEN 'regular'
            ELSE 'occasional'
        END as CustomerSegment
    FROM BikeShare.staging_citibiketrips
    WHERE NOT EXISTS (SELECT 1 FROM BikeShare.dim_member WHERE dim_member.MemberType = member_casual);

    -- Populate Weather Dimension
    INSERT INTO BikeShare.dim_weather (DateKey, TimeKey, Temperature, Humidity, Precipitation, WeatherCode, WeatherCondition, WindSpeed, FeelsLikeTemp)
    SELECT 
        w.date_key,
        w.time_key,
        w.temperature_c,
        w.humidity_percent,
        w.precipitation_mm,
        w.weather_code,
        w.weather_condition,
        w.wind_speed_kmh,
        w.temperature_c - (w.wind_speed_kmh * 0.1) as FeelsLikeTemp
    FROM BikeShare.staging_weatherdata w
    WHERE NOT EXISTS (
        SELECT 1 FROM BikeShare.dim_weather 
        WHERE dim_weather.DateKey = w.date_key AND dim_weather.TimeKey = w.time_key
    );

    -- Populate Fact Trips Table
    INSERT INTO BikeShare.fct_trips (
        RideID, StartDateKey, StartTimeKey, EndDateKey, EndTimeKey,
        StartStationKey, EndStationKey, BikeKey, MemberKey, WeatherKey,
        TripDurationMinutes, TripDistanceKm, StartLatitude, StartLongitude,
        EndLatitude, EndLongitude, SpeedKmh, ProcessedAt, ProcessingBatchID
    )
    SELECT 
        t.ride_id as RideID,
        CONVERT(INT, FORMAT(CAST(t.started_at AS DATE), 'yyyyMMdd')) as StartDateKey,
        DATEPART(HOUR, t.started_at) as StartTimeKey,
        CONVERT(INT, FORMAT(CAST(t.ended_at AS DATE), 'yyyyMMdd')) as EndDateKey,
        DATEPART(HOUR, t.ended_at) as EndTimeKey,
        ss.StationKey as StartStationKey,
        es.StationKey as EndStationKey,
        b.BikeKey,
        m.MemberKey,
        w.WeatherKey,
        DATEDIFF(MINUTE, t.started_at, t.ended_at) as TripDurationMinutes,
        -- Calculate approximate distance using Haversine formula
        6371 * 2 * ASIN(SQRT(
            POWER(SIN((RADIANS(t.end_lat) - RADIANS(t.start_lat)) / 2), 2) +
            COS(RADIANS(t.start_lat)) * COS(RADIANS(t.end_lat)) *
            POWER(SIN((RADIANS(t.end_lng) - RADIANS(t.start_lng)) / 2), 2)
        )) as TripDistanceKm,
        t.start_lat as StartLatitude,
        t.start_lng as StartLongitude,
        t.end_lat as EndLatitude,
        t.end_lng as EndLongitude,
        CASE 
            WHEN DATEDIFF(MINUTE, t.started_at, t.ended_at) > 0 
            THEN (6371 * 2 * ASIN(SQRT(
                POWER(SIN((RADIANS(t.end_lat) - RADIANS(t.start_lat)) / 2), 2) +
                COS(RADIANS(t.start_lat)) * COS(RADIANS(t.end_lat)) *
                POWER(SIN((RADIANS(t.end_lng) - RADIANS(t.start_lng)) / 2), 2)
            ))) / (DATEDIFF(MINUTE, t.started_at, t.ended_at) / 60.0)
            ELSE 0
        END as SpeedKmh,
        GETDATE() as ProcessedAt,
        t.processing_batch_id as ProcessingBatchID
    FROM BikeShare.staging_citibiketrips t
    LEFT JOIN BikeShare.dim_station ss ON t.start_station_id = ss.StationID
    LEFT JOIN BikeShare.dim_station es ON t.end_station_id = es.StationID
    LEFT JOIN BikeShare.dim_bike b ON t.rideable_type = b.RideableType
    LEFT JOIN BikeShare.dim_member m ON t.member_casual = m.MemberType
    LEFT JOIN BikeShare.dim_weather w ON CONVERT(INT, FORMAT(CAST(t.started_at AS DATE), 'yyyyMMdd')) = w.DateKey 
        AND DATEPART(HOUR, t.started_at) = w.TimeKey
    WHERE YEAR(CAST(t.started_at AS DATE)) = @LoadYear 
        AND MONTH(CAST(t.started_at AS DATE)) = @LoadMonth;

END