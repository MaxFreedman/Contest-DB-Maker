import aiohttp
import asyncio
import os
import sqlite3

# Define the URL templates with identifiers
url_templates = [
    ("https://cqww.com/publiclogs/{year}cw/{call_sign}.log", 'cw'),
    ("https://cqww.com/publiclogs/{year}ph/{call_sign}.log", 'ssb'),
    ("https://cqwwrtty.com/publiclogs/{year}/{call_sign}.log", 'rtty')
]

# Read the call signs from list.txt
with open('callsigns.txt', 'r') as file:
    call_signs = [line.strip().lower() for line in file]

# Create a directory to save the logs if it doesn't exist
os.makedirs('logs', exist_ok=True)

async def download_and_process_log(semaphore, session, year, call_sign):
    async with semaphore:
        success = False  # Flag to check if at least one log was downloaded
        for url_template, identifier in url_templates:
            url = url_template.format(year=year, call_sign=call_sign)
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        text = await response.text()
                        db_filename = f'logs/{call_sign}_{year}_{identifier}.db'
                        conn = sqlite3.connect(db_filename)
                        cursor = conn.cursor()

                        # Create tables with unique constraints
                        cursor.execute('''
                            CREATE TABLE IF NOT EXISTS header (
                                key TEXT,
                                value TEXT,
                                UNIQUE(key, value)
                            )
                        ''')
                        cursor.execute('''
                            CREATE TABLE IF NOT EXISTS qso (
                                frequency TEXT,
                                mode TEXT,
                                date TEXT,
                                time TEXT,
                                my_call TEXT,
                                my_rst TEXT,
                                my_zone TEXT,
                                their_call TEXT,
                                their_rst TEXT,
                                their_zone TEXT,
                                UNIQUE(frequency, mode, date, time, my_call, their_call)
                            )
                        ''')

                        log_data = text.splitlines()
                        header_data = []
                        qso_data = []
                        header_section = True

                        for line in log_data:
                            if line.startswith('QSO:'):
                                header_section = False

                            if header_section:
                                if ': ' in line:
                                    key, value = line.split(': ', 1)
                                    header_data.append((key, value))
                            else:
                                if line.startswith('QSO:'):
                                    parts = line.split()
                                    if len(parts) >= 11:
                                        qso_data.append((
                                            parts[1],  # frequency
                                            parts[2],  # mode
                                            parts[3],  # date
                                            parts[4],  # time
                                            parts[5],  # my_call
                                            parts[6],  # my_rst
                                            parts[7],  # my_zone
                                            parts[8],  # their_call
                                            parts[9],  # their_rst
                                            parts[10]  # their_zone
                                        ))

                        cursor.executemany('INSERT OR IGNORE INTO header (key, value) VALUES (?, ?)', header_data)
                        cursor.executemany('''
                            INSERT OR IGNORE INTO qso (
                                frequency, mode, date, time, my_call, my_rst, my_zone, 
                                their_call, their_rst, their_zone
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', qso_data)

                        conn.commit()
                        conn.close()
                        print(f'Successfully downloaded and saved {db_filename} from {url}')
                        success = True  # Set the flag to True after a successful download
                    else:
                        print(f'Failed to download from {url}')
            except Exception as e:
                print(f'Error downloading from {url}: {e}')

        if not success:
            print(f'Failed to download any logs for {call_sign} in {year}')

async def main():
    semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent tasks
    async with aiohttp.ClientSession() as session:
        tasks = []
        for year in range(2005, 2023 + 1):
            for call_sign in call_signs:
                tasks.append(download_and_process_log(semaphore, session, year, call_sign))
        await asyncio.gather(*tasks)

# Run the main function
asyncio.run(main())