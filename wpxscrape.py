import aiohttp
import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, UniqueConstraint
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

#envs
load_dotenv()

# Define the URL templates with identifiers
url_templates = [
    ("https://cqwpx.com/publiclogs/{year}cw/{call_sign}.log", 'cw'),
    ("https://cqwpx.com/publiclogs/{year}ph/{call_sign}.log", 'ph'),
    ("https://cqwpxrtty.com/publiclogs/{year}/{call_sign}.log", 'rtty')
]

# Read the call signs from list.txt
with open('callsigns.txt', 'r') as file:
    call_signs = [line.strip().lower() for line in file]

# SQLAlchemy setup
DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_async_engine(DATABASE_URL, echo=True)
Base = declarative_base()

class Header(Base):
    __tablename__ = 'header'
    key = Column(String, primary_key=True)
    value = Column(String, primary_key=True)
    __table_args__ = (UniqueConstraint('key', 'value', name='_key_value_uc'),)

class QSO(Base):
    __tablename__ = 'qso'
    frequency = Column(String, primary_key=True)
    mode = Column(String, primary_key=True)
    date = Column(String, primary_key=True)
    time = Column(String, primary_key=True)
    my_call = Column(String, primary_key=True)
    their_call = Column(String, primary_key=True)
    my_rst = Column(String)
    my_serial = Column(String)
    their_rst = Column(String)
    their_serial = Column(String)
    __table_args__ = (UniqueConstraint('frequency', 'mode', 'date', 'time', 'my_call', 'their_call', name='_qso_uc'),)

async def download_and_process_log(semaphore, session, year, call_sign):
    async with semaphore:
        success = False  # Flag to check if at least one log was downloaded
        for url_template, identifier in url_templates:
            url = url_template.format(year=year, call_sign=call_sign)
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        text = await response.text()
                        async with AsyncSession(engine) as db_session:
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
                                        header_data.append(Header(key=key, value=value))
                                else:
                                    if line.startswith('QSO:'):
                                        parts = line.split()
                                        if len(parts) >= 11:
                                            qso_data.append(QSO(
                                                frequency=parts[1],  # frequency
                                                mode=parts[2],  # mode
                                                date=parts[3],  # date
                                                time=parts[4],  # time
                                                my_call=parts[5],  # my_call
                                                my_rst=parts[6],  # my_rst
                                                my_serial=parts[7],  # my_serial
                                                their_call=parts[8],  # their_call
                                                their_rst=parts[9],  # their_rst
                                                their_serial=parts[10]  # their_serial
                                            ))

                            db_session.add_all(header_data)
                            db_session.add_all(qso_data)
                            await db_session.commit()
                        print(f'Successfully downloaded and saved logs for {call_sign} from {url}')
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