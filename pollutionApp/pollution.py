from django.db import connection
from django.http import JsonResponse  # Import JsonResponse
from rest_framework.views import APIView



# query = """
#     SELECT *
#     FROM public.pocairpollution_latest
#     WHERE "end" <= (SELECT MAX("end") FROM public.pocairpollution_latest)
#     AND start >= (SELECT MAX("end") FROM public.pocairpollution_latest) - interval %s
#     AND state = %s;
# """
query = """
 SELECT start, "end", state, count_pollutants_co2, sum_pollutants_co2, avg_pollutants_co2, aggr_param, aggr_value 
    FROM public.pocairpollution_integration_persist 
    WHERE "end" <= (SELECT MAX("end") FROM public.pocairpollution_integration_persist)
    AND start >= (SELECT MAX("end") FROM public.pocairpollution_integration_persist) - interval %s
"""
class GetAirQuality(APIView):
    def get(self, request):
        try:
            # Execute the raw SQL query
            state = request.query_params.get("state")
            time_interval = request.query_params.get("time_interval")
            city = request.query_params.get("city")
            # agg_param1 = 'City'
            region = request.query_params.get("region")
            # agg_param2 = 'Region'
            with connection.cursor() as cursor:
                cursor.execute(query,(time_interval,))
                rows = cursor.fetchall()


                # Process rows to format the response data
                data = []
                for row in rows:
                    data.append({
                        'start': row[0],
                        'end': row[1],
                        'state':row[2],
                        'count_pollutants_co2':row[3],
                        'sum_pollutants_co2':row[4],
                        'avg_pollutants_co2':row[5],
                        'aggr_param':row[6],
                        'aggr_value':row[7],
                    })
                # filtered_data = [
                #     row for row in data
                #     # if (city.lower() in row['aggr_value'].lower() if city else True) and
                #     #    (region.lower() in row['aggr_value'].lower() if region else True)
                #     if ( state !== 'All States' and  row['state'.lower ==  and state]) or
                #     (row['aggr_param'].lower() == 'city' and city.lower() in row['aggr_value'].lower() and city ) or
                #        (row['aggr_param'].lower() == 'region' and region.lower() in row['aggr_value'].lower() and region)
                # ]

                filtered_data = [
                    row for row in data
                    if (
                            (state.lower() == 'all states' or row['state'].lower() == state.lower()) and
                            ((city.lower() == 'all cities' or (row['aggr_param'].lower() == 'city' and city.lower() in row['aggr_value'].lower())) or
                            (region.lower() == 'all regions' or (row['aggr_param'].lower() == 'region' and region.lower() in row['aggr_value'].lower())))
                    )
                ]

                return JsonResponse(filtered_data, safe=False)

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
