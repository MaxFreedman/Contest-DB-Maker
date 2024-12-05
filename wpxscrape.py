import aiohttp
import asyncio
import os
import re
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, String, Integer, ForeignKey, UniqueConstraint, Text
)
from sqlalchemy.orm import relationship
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

class Log(Base):
    __tablename__ = 'logs'
    log_id = Column(Integer, primary_key=True, autoincrement=True)
    start_of_log = Column(String, nullable=True)
    contest = Column(String, nullable=True)
    callsign = Column(String, nullable=True)
    location = Column(String, nullable=True)
    category_operator = Column(String, nullable=True)
    category_assisted = Column(String, nullable=True)
    category_band = Column(String, nullable=True)
    category_power = Column(String, nullable=True)
    category_mode = Column(String, nullable=True)
    category_transmitter = Column(String, nullable=True)
    category_station = Column(String, nullable=True)
    category_overlay = Column(String, nullable=True)
    grid_locator = Column(String, nullable=True)
    claimed_score = Column(String, nullable=True)
    name = Column(String, nullable=True)
    club = Column(String, nullable=True)
    created_by = Column(String, nullable=True)
    soapbox = Column(Text, nullable=True)  # Soapbox can have multiple lines/text

    # Relationships
    qsos = relationship('QSO', back_populates='log')

class QSO(Base):
    __tablename__ = 'qso'
    id = Column(Integer, primary_key=True, autoincrement=True)
    log_id = Column(Integer, ForeignKey('logs.log_id'))
    frequency = Column(String, nullable=True)
    mode = Column(String, nullable=True)
    date = Column(String, nullable=True)
    time = Column(String, nullable=True)
    my_call = Column(String, nullable=True)
    their_call = Column(String, nullable=True)
    my_rst = Column(String, nullable=True)
    my_serial = Column(String, nullable=True)
    their_rst = Column(String, nullable=True)
    their_serial = Column(String, nullable=True)
    extra_field = Column(String, nullable=True)

    # Relationships
    log = relationship('Log', back_populates='qsos')

    __table_args__ = (
        UniqueConstraint('log_id', 'frequency', 'mode', 'date', 'time', 'my_call', 'their_call', name='_qso_uc'),
    )

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

async def download_and_process_log(semaphore, http_session, year, call_sign):
    async with semaphore:
        for url_template, identifier in url_templates:
            url = url_template.format(year=year, call_sign=call_sign)
            try:
                async with http_session.get(url) as response:
                    if response.status == 200:
                        text = await response.text()
                        async with AsyncSession(engine) as db_session:
                            log_data = text.splitlines()
                            header_section = True

                            # Create a new Log instance
                            log = Log()

                            soapbox_entries = []

                            for line in log_data:
                                if line.startswith('QSO:'):
                                    header_section = False
                                    # Process the QSO line
                                    parts = re.split(r'\s+', line)
                                    # Adjust the parsing based on the number of parts
                                    if len(parts) >= 10:
                                        qso = QSO(
                                            log=log,
                                            frequency=parts[1],
                                            mode=parts[2],
                                            date=parts[3],
                                            time=parts[4],
                                            my_call=parts[5],
                                            my_rst=parts[6],
                                            my_serial=parts[7],
                                            their_call=parts[8],
                                            their_rst=parts[9],
                                            their_serial=parts[10],
                                            extra_field=parts[11] if len(parts) > 11 else None
                                        )
                                        db_session.add(qso)
                                    else:
                                        print(f"Line skipped due to insufficient data: {line}")
                                elif header_section:
                                    # Handle headers
                                    if ': ' in line:
                                        key, value = line.split(': ', 1)
                                        key_lower = key.lower().replace('-', '_')

                                        # Assign value to the corresponding attribute if it exists
                                        if hasattr(log, key_lower):
                                            setattr(log, key_lower, value)
                                        elif key_lower == 'soapbox':
                                            soapbox_entries.append(value)
                                    else:
                                        # Handle SOAPBOX entries without ': '
                                        if line.strip():
                                            soapbox_entries.append(line.strip())
                                else:
                                    # Optionally handle any other lines after headers
                                    pass

                            # Combine soapbox entries
                            if soapbox_entries:
                                log.soapbox = '\n'.join(soapbox_entries)

                            # Assign default values if necessary
                            log.callsign = log.callsign or call_sign.upper()
                            log.contest = log.contest or identifier.upper()

                            # Add the log to the session
                            db_session.add(log)
                            await db_session.commit()
                        print(f'Successfully processed logs for {call_sign} from {url}')
                    else:
                        print(f'Failed to download from {url} with status {response.status}')
            except Exception as e:
                print(f'Error downloading from {url}: {e}')

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