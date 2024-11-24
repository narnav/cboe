from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from your_module import OptionTrade  # Replace 'your_module' with the module where `OptionTrade` is defined

# Connect to SQLite database
engine = create_engine('sqlite:///option_trades.db')

# Prepare session
Session = sessionmaker(bind=engine)
session = Session()

# Query parameters
strike_value = 480
option_type_value = 'P'
expiration_date = '2024-01-02'

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
        f"Trade Price: {trade.trade_price}"
    )
