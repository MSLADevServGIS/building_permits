
import os
import tempfile
from time import sleep
import urllib
import urlparse
import webbrowser

import folium


# Source: https://stackoverflow.com/questions/11687478
def path2url(path):
    return urlparse.urljoin('file:', urllib.pathname2url(path))


def make_temp_map(json_data, loc=[43, -100], zoom=3):
    m = folium.Map(location=loc, zoom_start=zoom)
    folium.GeoJson(open(json_data)).add_to(m)
    t = tempfile.NamedTemporaryFile(suffix=".html", delete=False)
    m.save(t)
    # Set size limit -- large html files crash browsers
    if int(os.stat(t.name).st_size/1000) < 10000:
        webbrowser.open(path2url(t.name))
        sleep(5)
    else:
        print("File size exceeded limit (10000 bytes).")
    t.close()
    os.remove(t.name)
    return
    


#make_temp_map("nhoods.json", loc=[46.855,-114.04], zoom=12)
#make_temp_map("addrs.json", loc=[46.855,-114.04], zoom=12)
