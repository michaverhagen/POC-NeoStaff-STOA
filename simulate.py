#!/usr/bin/env python3
""" load Galley data """

import time
import datetime
import yaml
from modules.activity import daily_activity_check
from modules.utilities import get_weaviate_client


def _check_activity(today=None):
    print(today)
    with open('./config.yml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        daily_activity_check(config, date=today)


def _simulate(delay):

    now = datetime.datetime.now()
    for count in range(0, 365):
        current = now + datetime.timedelta(days=count)
        _check_activity(today=current)
        time.sleep(delay)
        if count > 365:
            break


#########################################################################################################
# only the call for the main function below this line
#########################################################################################################


def main():
    """ main """
    start = time.time()

    _simulate(0.1)

    end = time.time()
    minutes = round((end-start)/60)
    print("Total time required ------------------------:", minutes, "minutes", round((end-start)%60, 1), "seconds")


if __name__ == '__main__':
    main()
