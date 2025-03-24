from django.db import connection
from django.http import JsonResponse  # Import JsonResponse
from rest_framework.views import APIView
from pollutionApp.queries import *

# query = """
#     SELECT *
#     FROM public.pocairpollution_latest
#     WHERE "end" <= (SELECT MAX("end") FROM public.pocairpollution_latest)
#     AND start >= (SELECT MAX("end") FROM public.pocairpollution_latest) - interval %s
#     AND state = %s;
# """




class GetStates(APIView):
    def get(self, request):
        try:
            with connection.cursor() as cursor:
                cursor.execute(get_states)
                rows = cursor.fetchall()
                states = []
                for row in rows:
                    states.append({
                        'state_id': row[0],
                        'state_name': row[1]
                    })
                return JsonResponse(states, safe=False)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

class GetCities(APIView):
    def get(self, request):
        try:
            state = request.query_params.get("state")

            with connection.cursor() as cursor:
                cursor.execute(get_cities, (state,))
                rows = cursor.fetchall()
                cities = []
                for row in rows:
                    cities.append({
                        'city_id': row[0],
                        'city_name': row[1]
                    })
                return JsonResponse(cities, safe=False)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)


class GetRegions(APIView):
    def get(self, request):
        try:
            city = request.query_params.get("city")
            with connection.cursor() as cursor:
                cursor.execute(get_regions, (city,))
                rows = cursor.fetchall()
                regions = []
                for row in rows:
                    regions.append({
                        'region_id': row[0],
                        'region_name': row[1]
                    })
                return JsonResponse(regions, safe=False)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)


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
            params = (state, state, city, state, city, region, region,time_interval)
            with connection.cursor() as cursor:
                cursor.execute(get_pollutants_data, params)
                rows = cursor.fetchall()

                # Process rows to format the response data
                data = []
                for row in rows:
                    data.append({
                        'start': row[0],
                        'end': row[1],
                        'state': row[2],
                        'count_pollutants_co2': row[3],
                        'sum_pollutants_co2': row[4],
                        'avg_pollutants_co2': row[5],
                        'aggr_param': row[6],
                        'aggr_value': row[7],
                    })


                filtered_data =[

                ]



                # filtered_data = [
                #     row for row in data
                #     if (
                #         # If state is 'allstates', no filtering is required as we get all states data
                #         (state.lower() == 'allstates') or
                #         (row['state'].lower() == state.lower() and city.lower() == 'allcities') or
                #
                #             # If city is 'allcities', filter based on state and city (region filter is not required here)
                #             (city.lower() == 'allcities' and row['state'].lower() == state.lower())
                #             # If region is 'allregions', filter with state, city, and region
                #             # ((region.lower() == 'allregions' and row['aggr_value'].lower() == state.lower() and
                #             #  (row['aggr_param'].lower() == 'city' and city.lower() in row['aggr_value'].lower())) or
                #             #
                #             #  (row['state'].lower() == state.lower() and (row['aggr_param'].lower() == 'city' and city.lower() in row['aggr_value'].lower()) or
                #             #   (row['aggr_param'].lower() == 'region' and region.lower() in row['aggr_value'].lower())))
                #
                #             # ((region.lower() == 'allregions' and row['state'].lower() == state.lower()) or
                #             #
                #             #  (row['state'].lower() == state.lower() and (row['aggr_param'].lower() == 'region'
                #             #                                              and region.lower() in row[
                #             #                                                  'aggr_value'].lower()))
                #             #  )
                #     )
                # ]

                return JsonResponse(data, safe=False)

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
