
import argparse
import os
import time
import sys

from selenium import webdriver
# from selenium.webdriver.common.keys import Keys

args = sys.argv[1:]


# URLs to "Report Generators"
URLS = {
    "building": ("http://cpdbprod/ReportServer/Pages/ReportViewer.aspx"
                 "?%2fLand%2fStatistics%2fNew+Construction+Report&rs:"
                 "Command=Render"),
    "business": None
    }


BUSINESS_LICENSES = None

# Destination folders for output permit reports
output_folders = {
    "building": {
        "city": ("//cityfiles/DEVServices/WallyG/projects/building_permits"
                 "/data/city_permits/raw"),
        "county": ("//cityfiles/DEVServices/WallyG/projects/building_permits"
                   "/data/county_permits/raw")
        },
    "business": None
    }

elements = {
    "start": "ReportViewerControl$ctl04$ctl03$txtValue",
    "end": "ReportViewerControl$ctl04$ctl05$txtValue",
    "submit": "ReportViewerControl$ctl04$ctl00",
    "save_as": "ReportViewerControl_ctl05_ctl04_ctl00_ButtonImg",
    "excel": '//*[@title="Excel"]'
    }


def get_permits(report_name, out, from_date, to_date):
    """Retrieves permit data from report generator."""
    profile = webdriver.ChromeOptions()
    profile.add_argument("--disable-gpu")
    profile.add_experimental_option(
        "prefs", {"download.default_directory": out})

    driver = webdriver.Chrome(os.environ["chromedriver"],
                              chrome_options=profile)

    url = URLS[report_name]
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


if __name__ == "__main__":
    get_permits(args[0], output_folders["building"]["city"], args[1], args[2])
