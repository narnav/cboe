import csv
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

# Define SQLAlchemy base
Base = declarative_base()

# Define the model for the table
class OptionTrade(Base):
    __tablename__ = 'option_trades'
    id = Column(Integer, primary_key=True, autoincrement=True)
    underlying_symbol = Column(String)
    quote_datetime = Column(String)
    sequence_number = Column(Integer)
    root = Column(String)
    expiration = Column(String)
    strike = Column(Float)
    option_type = Column(String)
    exchange_id = Column(Integer)
    trade_size = Column(Integer)
    trade_price = Column(Float)
    trade_condition_id = Column(Integer)
    canceled_trade_condition_id = Column(Integer)
    best_bid = Column(Float)
    best_ask = Column(Float)
    trade_iv = Column(Float)
    trade_delta = Column(Float)
    underlying_bid = Column(Float)
    underlying_ask = Column(Float)
    number_of_exchanges = Column(Integer)

# Connect to SQLite database
engine = create_engine('sqlite:///option_trades.db')
Base.metadata.create_all(engine)

# Prepare session
Session = sessionmaker(bind=engine)
session = Session()

# File path for the CSV
file_path = "data/xsp_2024-01.csv"
def get_in():
    # Parse CSV and insert rows
    with open(file_path, mode='r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            trade = OptionTrade(
                underlying_symbol=row[0],
                quote_datetime=row[1],#'quote_datetime'],
                sequence_number=row[2],#]'sequence_number']),
                root=row[3],# 'root'],
                expiration=row[4],#)#'expiration'],
                strike=float(row[5]),#'strike']),
                option_type=row[6],#'option_type'],
                exchange_id=int(row[7]),#'exchange_id']),
                trade_size=int(row[8]),#'trade_size']),
                trade_price=float(row[9]),#'trade_price']),
                trade_condition_id=int(row[10]),#'trade_condition_id']),
                canceled_trade_condition_id=int(row[11]),#'canceled_trade_condition_id']),
                best_bid=float(row[12]),#'best_bid']),
                best_ask=float(row[13]),#'best_ask']),
                trade_iv=float(row[14]),#'trade_iv']),
                trade_delta=float(row[15]),#'trade_delta']),
                underlying_bid=float(row[16]),#'underlying_bid']),
                underlying_ask=float(row[17]),#'underlying_ask']),
                number_of_exchanges=int(row[18]),#'number_of_exchanges']),
            )
            session.add(trade)
            # session.commit()
    # Commit the session
    session.commit()

    print("Data inserted successfully!")


def get_data(strike_value = 480,    option_type_value = 'P',    expiration_date = '2024-01-02',output_file="output.csv",file_mode='w'):
    start_time = datetime(2024, 1,17, 9, 00, 0)
    end_time = datetime(2024, 1,  17, 16, 00, 0)
    # Query the database
    results = (
        session.query(OptionTrade)
        .filter(
            OptionTrade.strike == strike_value,
            OptionTrade.option_type == option_type_value,
            OptionTrade.expiration == expiration_date,
            OptionTrade.quote_datetime.between(start_time, end_time)
        )
        .order_by(OptionTrade.quote_datetime)
        .all()
    )

    # Print the results
    for trade in results:
        print(
            f"Quote Datetime: {trade.quote_datetime}, "
            f"Strike: {trade.strike}, "
            f"Option Type: {trade.option_type}, "
            f"Trade Size: {trade.trade_size}, "
            f"expiration_date: {trade.expiration}, "
            f"Trade Price: {trade.trade_price}",
            f"XSP: {trade.underlying_bid}"
            
        )
     # Write results to a CSV file
    with open(output_file, mode=file_mode, newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header row
        writer.writerow([
            "Quote Datetime",
            "Strike",
            "exp",
            "Trade Size",
            "Trade Price",
            "XSP",
        ])
        
        # Write data rows
        for trade in results:
            writer.writerow([
                trade.quote_datetime,
                f"{trade.option_type}{trade.strike}  ",
                trade.expiration ,
                trade.trade_size,
                trade.trade_price,
                trade.underlying_bid,
            ])
    
    print(f"Data written to {output_file}")


get_data(strike_value = 480,    option_type_value = 'P',    expiration_date = '2024-01-17',file_mode='w')
get_data(strike_value = 479,    option_type_value = 'P',    expiration_date = '2024-01-22',file_mode='a')
get_data(strike_value = 470,    option_type_value = 'P',    expiration_date = '2024-01-22',file_mode='a')
# get_data(strike_value = 465,    option_type_value = 'P',    expiration_date = '2024-01-17',file_mode='a')
# get_data(strike_value = 476,    option_type_value = 'P',    expiration_date = '2024-01-09',file_mode='a')


