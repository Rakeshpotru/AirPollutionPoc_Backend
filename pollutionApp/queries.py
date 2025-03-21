get_states = """
                SELECT state_id,state_name FROM public.states
            """

get_cities = """
                select city_id,city_name from public.cities where state_name = %s
            """

get_regions = """
                select region_id,region_name from public.regions where city_name = %s
            """
get_pollutants_data = """
 SELECT start, "end", state, count_pollutants_co2, sum_pollutants_co2, avg_pollutants_co2, aggr_param, aggr_value 
    FROM public.pocairpollution_integration_persist 
    WHERE "end" <= (SELECT MAX("end") FROM public.pocairpollution_integration_persist)
    AND start >= (SELECT MAX("end") FROM public.pocairpollution_integration_persist) - interval %s
"""