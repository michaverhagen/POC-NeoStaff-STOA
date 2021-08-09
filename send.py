#!/usr/bin/env python3
""" load Galley data """

import time
import yaml
from modules.sendmail import send_mail


TEMPLATE = "./data/templates/A1_bericht.html"
#ADDRESS = "micha.verhagen@gmail.com"
ADDRESS = "roland.bulder@neostaf.com"
SUBJECT = "A1 bericht"


def _send():
    send_mail(SUBJECT, TEMPLATE, ADDRESS)


#########################################################################################################
# only the call for the main function below this line
#########################################################################################################


def main():
    """ main """
    start = time.time()

    _send()

    end = time.time()
    minutes = round((end-start)/60)
    print("Total time required ------------------------:", minutes, "minutes", round((end-start)%60, 1), "seconds")


if __name__ == '__main__':
    main()
