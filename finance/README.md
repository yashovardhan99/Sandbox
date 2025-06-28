# nifty.py
nifty.py is a simple script which allows comparing SIP performance across various Nifty indices.

## Usage
1. Download the historical index data in CSV format from [niftyindices.com](https://niftyindices.com/reports/historical-data)
1. Copy all files in a `data/indices` folder. Ensure that all files use the same start/end date.
1. Run `python nifty.py <sip-amount> <step-up-%>` to get a nice summary of all the downloaded indices.

This script simulates a step-up SIP across multiple Nifty indices and displays the result in a nice format. The result includes the absolute gains, CAGR and XIRR across the date range for a monthly SIP starting on the first day.