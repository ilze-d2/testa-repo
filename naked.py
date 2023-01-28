#tiek importētas bibliotekas
import logging
import logging.config
import requests  
import json
import datetime
import time
import yaml

# Loading logging configuration
with open('./log_worker.yaml', 'r') as stream:
        log_config = yaml.safe_load(stream)

logging.config.dictConfig(log_config)

# Creating logger
logger = logging.getLogger('root')

#tiek importēta datetime klase un izdrukāts: Asteroid processing service
from datetime import datetime
from configparser import ConfigParser
logger.info('Asteroid processing service')

# Initiating and reading config values
#tiek izdrukāts: Loading configuration from file
logger.info('Loading configuration from file')


try:
	config = ConfigParser()
	config.read('config.ini')

	nasa_api_key = config.get('nasa', 'api_key')
	nasa_api_url = config.get('nasa', 'api_url')

except:
	logger.exception('')
logger.info('DONE')

# Getting todays date
#tiek izveidota fukcija, kura iegūst informāciju par šodienas datumu (gadu, mēnesi, dienu)
#tiek izdrukāts: Generated today's date:  un informācija par šodienas datumu
dt = datetime.now()
request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
logger.debug("Generated today's date: " + str(request_date))

#izmantojot NASA API URL adresi tiek izdrukāta adrese ,piemēram,:
#Request url:https://api.nasa.gov/neo/rest/v1/feed?start_date=2023-01-28&end_date=2023-01-28&api_key=FAXImnwMhMlumCiQ96i6qAwu1CqAqvEx7Bwwlnik 
logger.debug("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key))
r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)

#tiek izdrukāta informācija par savienojumu ar serveri, hedris un teksts
logger.debug("Response status code: " + str(r.status_code))
logger.debug("Response headers: " + str(r.headers))
logger.debug("Response content: " + str(r.text))

#tiek pārbaudīts vai pieprasījums ir bijis veiksmīgs un  ir savienojums ar severi
if r.status_code == 200:

	#metode, kura jason pārtūlko python
	json_data = json.loads(r.text)
	
	#tiek izveidots izveidoti saraksti ast_safe un ast_hazardous
	ast_safe = [] 
	ast_hazardous = []

	#tiek atrasts element_count un tiek izveidots mainīgais ast_count, kurā element_count skaitli uzglabā
	if 'element_count' in json_data:
		ast_count = int(json_data['element_count'])
		#tiek izdrukāta informāciaja par asteorīdiem šodien
		logger.info("Asteroid count today: " + str(ast_count))

		#tiek pārbaudīts vai ast_count ir lielāks par 0
		if ast_count > 0:
			#ja nosacījums (ast_count lielāks par 0) izpildās, tad tiek izveidots cikls, kur tiek iziets visām vērtībām pēc near_earth_objects 
			for val in json_data['near_earth_objects'][request_date]:
				#tiek atlasītas vērtības name, nasa_jpl_url, estimated_diameter, is_potentially_hazardous_asteroid, close_approach_data
				if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
					#tiek izveidots mainīgais tmp_ast_name, kurā tiek uzglabāti asteorīdu nosaukumi
					tmp_ast_name = val['name']
					#tiek izveidots mainīgais , kurā uzglabā informāciju  par adresi
					tmp_ast_nasa_jpl_url = val['nasa_jpl_url']
					#tiek pārbaudīta vai estimated_diameter ir kilometros 
					if 'kilometers' in val['estimated_diameter']:
						#tiek pārbaudīts vai ir vērtības estimated_diameter_min un estimated_diameter_max kilometros
						if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']:
							#tmp_ast_diam_min un tmp_ast_diam_max tiek noapaļotas līdz 3 cipariem aiz komata
							tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)
							tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3)
						else:
							#ja estimated_diameter_min un estimated_diameter_max nav kilometros,
							#izpildās else cikls un tmp_ast_diam_min untmp_ast_diam_max tiek pieņemts -2
							tmp_ast_diam_min = -2
							tmp_ast_diam_max = -2
					else:
						#ja estimated_diameter nav kilometros, tad
						#tiek pieņemtas tmp_ast_diam_min un tmp_ast_diam_max vērtības -1
						tmp_ast_diam_min = -1
						tmp_ast_diam_max = -1

					#tiek sagalabāta informācija mainīgajā tmp_ast_hazardous par is_potentially_hazardous_asteroid
					tmp_ast_hazardous = val['is_potentially_hazardous_asteroid']

					#tiek pārbaudīts asteorīdu zemes sasniegšanas datums, vai tas ir lieāks par 0
					if len(val['close_approach_data']) > 0:
					
						#tiek pārbaudīts vai close_approach_data satur
						#epoch_date_close_approach, relative_velocity, miss_distance 
						if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]:
							#tiek aprēķināta tmp_ast_close_appr_ts vērtība
							tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000)
							#tiek iegūta UTC datuma un laika vērtība tmp_ast_close_appr_dt_utc  
							tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
							#tiek iegūta datuma un laika vērtība tmp_ast_close_appr_dt  
							tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')

							#tiek pārbaudīts vai close_approach_data satur informāciju par relative_velocity kilometers_per_hour
							if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']:
								#tmp_ast_speed tiek pieņemta no iegūtās vērtības 
								tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']))
							else:
								#tmp_ast_speed tiek pieņemts kā -1
								tmp_ast_speed = -1

							#tiek pārbaudīts vai close_approach_data satur informāciju par miss_distance kilometers
							if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
								#tmp_ast_miss_dist tiek pieņemta un noapaļota līdz 3 cipariem aiz komata no iegūtās vērtības
								tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
							else:
								# tmp_ast_miss_dist tiek pieņemts kā -1
								tmp_ast_miss_dist = -1

						#ja datus nesatur, tad  tiek pieņemtas 
						#tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt vērtības
						else:
							tmp_ast_close_appr_ts = -1
							tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59"
							tmp_ast_close_appr_dt = "1969-12-31 23:59:59"
					#ja close_approach_data nav lielāks par 0
					else: 
						#tiek izdrukāts:No close approach data in message
						logger.warning("No close approach data in message")
						# tiek pieņemtas vērtības tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc,
						#tmp_ast_close_appr_dt, tmp_ast_speed, mp_ast_miss_dist 
						tmp_ast_close_appr_ts = 0
						tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
						tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
						tmp_ast_speed = -1
						tmp_ast_miss_dist = -1

					#tiek izdrukāta informācija par asteorīdu
					logger.info("------------------------------------------------------- >>")
					logger.info("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
					logger.info("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
					logger.info("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km")

					#ja tmp_ast_hazardous  vērtība ir True, tad tiek  pievienoti dati ast_hazardous
					# Adding asteroid data to the corresponding array
					if tmp_ast_hazardous == True:
						ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])
					#ja tmp_ast_hazardous  vērtība nav True, tad tiek  pievienoti dati ast_safe
					else:
						ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])

		#ja ast_count nav lielāks par 0, tad  tiek izdrukāts: No asteroids are going to hit earth today
		else:
			logger.info("No asteroids are going to hit earth today")

	#tiek izdrukāts Hazardous asteorids un Safe asteroids daudzums (garums)
	logger.info("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))

	#tiek pārbaudīts vai ast_hazardous garums ir lielāks par 0
	if len(ast_hazardous) > 0:

		#tiek sakartots saraksts ast_hazardous pēc tmp_ast_close_appr_ts elementa augošā secībā
		ast_hazardous.sort(key = lambda x: x[4], reverse=False)

		#tiek izdrukāts: Today's possible apocalypse (asteroid impact on earth) times:
		logger.info("Today's possible apocalypse (asteroid impact on earth) times:")
		#tiek veikta iterācija  sarakstā  un tiek izdrukāta informācija 
		#tmp_ast_close_appr_dt, tmp_ast_name,| more info: ,tmp_ast_nasa_jpl_url
		for asteroid in ast_hazardous:
			print(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))

		#tiek sakartots saraksts ast_hazardous pēc elementa tmp_ast_miss_dist  augošā secībā
		ast_hazardous.sort(key = lambda x: x[8], reverse=False)
		#tiek izdrukāts Closest passing distance is for:, tmp_ast_name, at:,tmp_ast_miss_dist ,km | more info:,tmp_ast_nasa_jpl_url 
		logger.info("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))

	#ja ast_hazardous garums ir mazāks par 0
	else:
		#tiek izdrukāta informācija par to, ka šodien neviens bītams asteroīds zemei garām nelido
		logger.info("No asteroids close passing earth today")

else:
	#tiek izdrukāta informācija par to, ka šobrīd nevar savienoties ar NASA API un tiek izdrukāta informacija
	logger.error("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
