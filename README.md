# Loone_forecast_boundary_GeoGlows
## Automated Forecasted Flow Data Download and Bias Correction

This project automates the daily download of forecasted flow data for specific stations identified for the Lake Okeechobee system. It leverages a script created by the Aquaveo team  (`[loone_data_prep](https://pypi.org/project/loone-data-prep/)`) and includes an additional bias correction function that adjusts the forecasted flows based on historical data from DBHYDRO.

## Features

- **Daily Automation:** The script runs daily at 4 am EST and downloads the forecasted flow data automatically.
- **Bias Correction:** Historical data is downloaded initially and used to correct the forecasted data.
- **GitHub Actions Integration:** The automation is set up using GitHub Actions, ensuring the task runs reliably every day.

