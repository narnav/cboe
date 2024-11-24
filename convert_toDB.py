import csv
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

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

    # Query the database
    results = (
        session.query(OptionTrade)
        .filter(
            OptionTrade.strike == strike_value,
            OptionTrade.option_type == option_type_value,
            OptionTrade.expiration == expiration_date
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
            f"Trade Price: {trade.trade_price}"
        )
     # Write results to a CSV file
    with open(output_file, mode=file_mode, newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header row
        writer.writerow([
            "Quote Datetime",
            "Strike",
            "Option Type",
            "Trade Size",
            "Trade Price",
        ])
        
        # Write data rows
        for trade in results:
            writer.writerow([
                trade.quote_datetime,
                trade.strike,
                trade.option_type,
                trade.trade_size,
                trade.trade_price,
            ])
    
    print(f"Data written to {output_file}")


get_data(strike_value = 475,    option_type_value = 'P',    expiration_date = '2024-01-31',file_mode='w')
get_data(strike_value = 467,    option_type_value = 'P',    expiration_date = '2024-01-31',file_mode='a')




# def get_rows_by_criteria(date, strike, option_type):
#     """
#     Fetch rows from the database based on date, strike, and option type.

#     Args:
#         date (str): The expiration date (e.g., '2024-01-02').
#         strike (float): The strike price (e.g., 480.0).
#         option_type (str): The option type ('C' or 'P').

#     Returns:
#         list: A list of rows that match the criteria.
#     """
#     results = (
#         session.query(OptionTrade)
#         .filter(
#             OptionTrade.expiration == date,
#             OptionTrade.strike == strike,
#             OptionTrade.option_type == option_type
#         )
#         .order_by(OptionTrade.quote_datetime)
#         .all()
#     )
#     return results


# # Example Usage
# rows = get_rows_by_criteria('2024-01-31', 480.0, 'P')

# # Print the fetched rows
# for row in rows:
#     print(
#         f"Quote Datetime: {row.quote_datetime}, "
#         f"Strike: {row.strike}, "
#         f"Option Type: {row.option_type}, "
#         f"Trade Size: {row.trade_size}, "
#         f"Trade Price: {row.trade_price}"
#     )
