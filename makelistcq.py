import logging
from bs4 import BeautifulSoup
import requests

#logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of URLs to scrape. Uncomment out ones you want
urls = [
   # ("https://cqww.com/publiclogs/{year}cw/", 'wwcw'),
   # ("https://cqww.com/publiclogs/{year}ph/", 'wwssb'),
   # ("https://cqwwrtty.com/publiclogs/{year}", 'wwrtty')
    ("https://cqwpx.com/publiclogs/{year}cw/", 'wpxcw'),
    ("https://cqwpx.com/publiclogs/{year}ph/", 'wpxssb'),
    ("https://cqwpxrtty.com/publiclogs/{year}", 'wpxrtty'),
   # ("https://cq160.com/publiclogs/{year}cw/", '160cw'),
   # ("https://cq160.com/publiclogs/{year}ph/", '160ssb'),
   # ("https://ww-digi.com/publiclogs/{year}", 'wwdigi'),
]

# Define the year range. Change as needed
start_year = 2005
end_year = 2023

callsigns = []

for year in range(start_year, end_year + 1):
    for url, mode in urls:
        formatted_url = url.format(year=year)
        logging.info(f"Fetching URL: {formatted_url}")
        response = requests.get(formatted_url)
        if response.status_code == 200:
            logging.info(f"Successfully fetched data for year {year} and mode {mode}")
            soup = BeautifulSoup(response.text, "html.parser")
            # Extract callsign from each <a> tag within <td> elements
            extracted_callsigns = [a.text for a in soup.find_all("a") if a.text.strip()]
            logging.info(f"Extracted {len(extracted_callsigns)} callsigns for year {year} and mode {mode}")
            callsigns.extend(extracted_callsigns)
        else:
            logging.error(f"Failed to fetch data for year {year} and mode {mode}, status code: {response.status_code}")

# Remove duplicates and sort the callsigns
unique_callsigns = sorted(set(callsigns))

# Write the unique and sorted callsigns to the file
with open('callsigns.txt', 'w') as file:
    for callsign in unique_callsigns:
        file.write(f"{callsign}\n")
    logging.info(f"Written {len(unique_callsigns)} unique callsigns to callsigns.txt")