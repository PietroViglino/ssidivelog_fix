import netCDF4 as nc
import requests
import logging
import json
import os

LOGGING_LEVEL = logging.INFO
NC_FILES_PATH = 'data'

assert os.path.exists(NC_FILES_PATH), f'Invalid file path {NC_FILES_PATH}'
    
def get_allids():
    logging.info('Retrieving the list of ids...')
    start = 0
    limit = 1000
    end_reached = False
    ids = []
    while not end_reached:
        url = f'http://192.168.30.103:12115/geo/allids?source=ssidivelog&start={start}&limit={limit}'
        resp = requests.get(url)
        json_data = json.loads(resp.content)
        if json_data == {
                "statusCode": 404,
                "message": "No data found",
                "error": "Not Found"
                }:
            end_reached = True
        else:
            ids += json_data
            start += 1000
    logging.info('Ids retrieved')
    return ids


def get_plat_name(mid):
    logging.debug(f'Getting platform code and sensor name for {mid}')
    url = f'http://192.168.30.103:12115/geo/byid/{mid}/emodnet?projection=properties.platformCode&projection=properties.sensor.name'
    resp = requests.get(url)
    json_data = json.loads(resp.content)
    plat_code = json_data['properties']['platformCode']
    sensor = json_data['properties']['sensor'][0]['name']
    return plat_code, sensor


def fix_nc(platformcode, name):
    if os.path.exists(os.path.join(NC_FILES_PATH, platformcode)):
        nc_files = [f for f in os.listdir(os.path.join(NC_FILES_PATH, platformcode)) if f.endswith('.nc')]
        for nc_file in nc_files: 
            try:
                logging.debug(f'Fixing {nc_file}')
                ncf = nc.Dataset(os.path.join(NC_FILES_PATH, platformcode, nc_file), 'r+')
                if 'TEMP' in ncf.variables:
                    ncf['TEMP'].sensor = name
                else:
                    logging.error(f"'TEMP' variable not found in {nc_file}.")
            finally:
                ncf.close()
                logging.info(f'Fixed {nc_file}')
    else:
        logging.debug(f'Couldn\'t find folder {NC_FILES_PATH}/{platformcode}')


def main():
    ids = get_allids()
    for doc in ids:
        try:
            plat_code, sensor = get_plat_name(doc['id'])
            fix_nc(plat_code, sensor)
        except Exception as e:
            logging.error(str(e))
    logging.info('Done')


if __name__ == '__main__':
    logger = logging.getLogger()    
    file_handler = logging.FileHandler(filename=f"logfile_ssidivelog_fix.log", mode="w")
    formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(LOGGING_LEVEL)
    main()