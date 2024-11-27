import aiohttp
import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, UniqueConstraint
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define the URL templates with identifiers
url_templates = [
    ("https://cqwpx.com/publiclogs/{year}cw/{call_sign}.log", 'cw'),
    ("https://cqwpx.com/publiclogs/{year}ph/{call_sign}.log", 'ph'),
    ("https://cqwpxrtty.com/publiclogs/{year}/{call_sign}.log", 'rtty')
]

# Read the call signs from callsigns.txt
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
    my_rst = Column(String, nullable=True)
    my_serial = Column(String, nullable=True)
    their_rst = Column(String, nullable=True)
    their_serial = Column(String, nullable=True)
    extra_field = Column(String, nullable=True)  # New field
    __table_args__ = (UniqueConstraint('frequency', 'mode', 'date', 'time', 'my_call', 'their_call', name='_qso_uc'),)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)  # Drop existing tables
        await conn.run_sync(Base.metadata.create_all)  # Create new tables

async def download_and_process_log(semaphore, http_session, year, call_sign):
    async with semaphore:
        success = False
        for url_template, identifier in url_templates:
            url = url_template.format(year=year, call_sign=call_sign)
            try:
                async with http_session.get(url) as response:
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
                                        # Handle both 11 and 12 field QSO lines
                                        if len(parts) == 12:
                                            # QSO line with extra_field
                                            _, frequency, mode, date, time, my_call, my_rst, my_serial, their_call, their_rst, their_serial, extra_field = parts
                                        elif len(parts) == 11:
                                            # QSO line without extra_field
                                            _, frequency, mode, date, time, my_call, my_rst, their_call, their_rst, their_serial, extra_field = parts
                                            # Assign None to extra_field or handle accordingly
                                            extra_field = None
                                        else:
                                            print(f"Line skipped due to unexpected number of fields ({len(parts)}): {line}")
                                            continue  # Skip lines that don't match expected formats

                                        qso = QSO(
                                            frequency=frequency,
                                            mode=mode,
                                            date=date,
                                            time=time,
                                            my_call=my_call,
                                            their_call=their_call,
                                            my_rst=my_rst,
                                            my_serial=my_serial if len(parts) >= 12 else None,
                                            their_rst=their_rst,
                                            their_serial=their_serial,
                                            extra_field=extra_field
                                        )
                                        qso_data.append(qso)

                            if header_data:
                                db_session.add_all(header_data)
                            if qso_data:
                                db_session.add_all(qso_data)
                            
                            await db_session.commit()
                        print(f'Successfully downloaded and saved logs for {call_sign} from {url}')
                        success = True
                    else:
                        print(f'Failed to download from {url} with status {response.status}')
            except Exception as e:
                print(f'Error downloading from {url}: {e}')

        if not success:
            print(f'Failed to download any logs for {call_sign} in {year}')

async def main():
    # Initialize the database
    await init_db()

    semaphore = asyncio.Semaphore(10)  # Limit to x concurrent tasks
    async with aiohttp.ClientSession() as http_session:
        tasks = []
        for year in range(2008, 2023 + 1):
            for call_sign in call_signs:
                tasks.append(download_and_process_log(semaphore, http_session, year, call_sign))
        await asyncio.gather(*tasks)

# Run the main function
asyncio.run(main())