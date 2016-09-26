#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""get_permits.py
This script <will be> the control mechanism for the permit spatialization
and reporting process.

Example Use:
python permits.py --update
python permits.py 2017
python permits.py 2017 --county
python permits.py --report
"""
import argparse
import os
import time
import sys
from datetime import datetime as dt

from selenium import webdriver
# from selenium.webdriver.common.keys import Keys

CURRENT_YEAR = dt.now().year

start_fmt = "01/01/{}"
end_fmt = "12/31/{}"

elements = {
    "start": "ReportViewerControl$ctl04$ctl03$txtValue",
    "end": "ReportViewerControl$ctl04$ctl05$txtValue",
    "submit": "ReportViewerControl$ctl04$ctl00",
    "save_as": "ReportViewerControl_ctl05_ctl04_ctl00_ButtonImg",
    "excel": '//*[@title="Excel"]'
    }

# Rename to download_permit_data
def get_permits(report_name, out, from_date, to_date):
    """Retrieves permit data from report generator."""
    try:
        driver_exe = os.environ["chromedriver"]
    except:
        print("This utility requires Google Chrome")
        print("It also needs a environment variable 'chromedriver'\n"
              "that is set to the location of chromedriver.exe")
        sys.exit(1)

    profile = webdriver.ChromeOptions()
    profile.add_argument("--disable-gpu")
    profile.add_experimental_option(
        "prefs", {"download.default_directory": out})

    driver = webdriver.Chrome(driver_exe, chrome_options=profile)

    url = ("http://cpdbprod/ReportServer/Pages/ReportViewer.aspx"
           "?%2fLand%2fStatistics%2fNew+Construction+Report&rs:"
           "Command=Render")
    driver.get(url)

    # Collect web elements (i.e. date inputs and submit button)
    from_date_elem = driver.find_element_by_name(elements["start"])
    to_date_elem = driver.find_element_by_name(elements["end"])
    view_report_elem = driver.find_element_by_name(elements["submit"])

    # Enter dates
    from_date_elem.clear()
    from_date_elem.send_keys(from_date)
    to_date_elem.clear()
    to_date_elem.send_keys(to_date)

    # Submit
    view_report_elem.click()
    time.sleep(1)

    # Download
    save_as_elem = driver.find_element_by_id(elements["save_as"])
    save_as_elem.click()

    excel_elem = save_as_elem.find_element_by_xpath(elements["excel"])
    excel_elem.click()
    time.sleep(3)

    # TODO: rename
    return


def reset_db(*args):
    pass

def update(*args):
    pass

def report():
    pass

def download_permit_data(*args):
    pass

def process_permit_data(*args):
    pass

def process_county_data(*args):
    pass


if __name__ == "__main__":
    # Arg Parser stuff
    parser = argparse.ArgumentParser(
        prog="permits.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__.split("\n\n")[0],
        epilog=__doc__.split("\n\n")[-1])
    # Most used
    parser.add_argument("year", action="store",
                        default=CURRENT_YEAR, nargs="?",
                        help="Append a year of permit data")
    parser.add_argument("--county", action="store_true", default=False,
                        help="Append a year of county permit data")
    parser.add_argument("--update", action="store_true", default=False,
                        dest="update", help="Update the spatial data")
    parser.add_argument("--report", action="store_true", default=False,
                        dest="report", help="Output reports")
    # Support a range of years
    parser.add_argument("--range", action="store_true", default=False,
                        dest="range",
                        help="Append permits from a range of years")
    parser.add_argument("--from", action="store", dest="yr_from",
                        type=int, help="Start year")
    parser.add_argument("--to", action="store", dest="yr_to", type=int,
                        default=CURRENT_YEAR, help="End year")
    parser.add_argument("--date", action="store", dest="date_to",
                        type=str, help="Specific date end")

    # Less often used
    parser.add_argument("--reset", action="store_true", default=False,
                        dest="reset", help="Append a year of permit data")

    args = parser.parse_args()

    if args.reset:
        reset_db()
        print("Database reset")
        sys.exit(0)

    elif args.update:
        update()
        print("Spatial data updated")
        sys.exit(0)

    elif args.report:
        report()
        print("Reports created")
        sys.exit(0)

    # Support range of years, specific end date
    elif args.range and args.yr_from:
        if args.date_to:
            args.yr_to = int(args.date_to.split("/")[-1])
            years = list(range(args.yr_from,
                               int(args.date_to.split("/")[-1])))
        else:
            years = list(range(args.yr_from, args.yr_to + 1))
        for year in years:
            download_permit_data(year)
            process_permit_data(year)
            print(start_fmt.format(year), end_fmt.format(year))
        if args.date_to:
            download_permit_data(args.yr_to, args.date_to)
            process_permit_data(args.yr_to, args.date_to)
            print(start_fmt.format(args.yr_to), args.date_to)
        sys.exit(0)

    # Process County data differently
    elif args.county:
        print("County {}".format(args.year))
        process_county_data(args.year)
        sys.exit(0)

    # Process one input year of City permit data
    elif len(sys.argv) == 2:
        print(args.year)
        download_permit_data(args.year)
        process_permit_data(args.year)
        sys.exit(0)

    else:
        raise AttributeError("Not enough parameters submitted\n"
                             "Use '-h' for help")
