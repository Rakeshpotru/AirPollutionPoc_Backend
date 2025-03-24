get_states = """
                SELECT state_id,state_name FROM public.states
            """

get_cities = """
                select city_id,city_name from public.cities where state_name = %s
            """

get_regions = """
                select region_id,region_name from public.regions where city_name = %s
            """
get_pollutants_data ="""
WITH max_end_date AS (
    SELECT MAX(p."end") AS max_end
    FROM public.pocairpollution_integration_persist_20_03_1 p
)
SELECT 
    p.start, 
    p.end, 
    p.state, 
    p.count_pollutants_co2, 
    p.sum_pollutants_co2, 
    p.avg_pollutants_co2, 
    p.aggr_param, 
    p.aggr_value
FROM 
    public.pocairpollution_integration_persist_20_03_1 p
JOIN 
    public.states s ON p.state = s.state_name

LEFT JOIN 
    public.regions r ON p.aggr_value = r.region_name
JOIN 
    max_end_date m ON p."end" <= m.max_end
WHERE 
    -- State filter (if 'allstates' is passed, no state filter)
    (%s = 'AllStates' OR p.state = %s)
    
    -- City filter (if 'allcities' is passed, no city filter)
    AND (
        %s = 'AllCities' 
        OR (p.aggr_param = 'City' AND p.aggr_value IN (SELECT city_name FROM public.cities WHERE state_name = %s))
        OR (p.aggr_param = 'Region' AND p.aggr_value IN (SELECT region_name FROM public.regions WHERE city_name = %s))
    )
    
    -- Region filter (if 'allregions' is passed, no region filter)
    AND (
        %s = 'AllRegions' 
        OR (p.aggr_param = 'Region' AND p.aggr_value = %s)
    )
    AND p.start >= m.max_end - interval %s
    AND p.end <= m.max_end
    ORDER BY p.start, p.end ASC;

"""

# JOIN
#     max_end_date m ON p."end" <= m.max_end
# WITH max_end_date AS (
#     SELECT MAX(p."end") AS max_end
#     FROM public.pocairpollution_integration_persist_20_03_1 p
# )
# AND p.start >= m.max_end - interval %s;

# -- Date range condition based on time interval
# AND p."end" <= (SELECT MAX(p."end") FROM public.pocairpollution_integration_persist_20_03_1)
# AND p.start >= (SELECT MAX(p."end") FROM public.pocairpollution_integration_persist_20_03_1) - interval %s;

# """
#  SELECT start, "end", state, count_pollutants_co2, sum_pollutants_co2, avg_pollutants_co2, aggr_param, aggr_value
#     FROM public.pocairpollution_integration_persist_20_03_1
#     WHERE "end" <= (SELECT MAX("end") FROM public.pocairpollution_integration_persist_20_03_1)
#     AND start >= (SELECT MAX("end") FROM public.pocairpollution_integration_persist_20_03_1) - interval %s
# """


