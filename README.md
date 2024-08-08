# Loone_forecast_boundary_GeoGlows
## Automated Forecasted Flow Data Download and Bias Correction

This project automates the daily download of forecasted flow data for specific stations identified for the Lake Okeechobee system. It leverages a script created by the Aquaveo team  (`[loone_data_prep](https://pypi.org/project/loone-data-prep/)`) and includes an additional bias correction function that adjusts the forecasted flows based on historical data from DBHYDRO.

## Features

- **Daily Automation:** The script runs daily at 4 am EST and downloads the forecasted flow data automatically.
- **Bias Correction:** Historical data is downloaded initially and used to correct the forecasted data.
- **GitHub Actions Integration:** The automation is set up using GitHub Actions, ensuring the task runs reliably every day.

## Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/YGC568/Loone_forecast_boundary_GeoGlows.git
   cd Loone_forecast_boundary_GeoGlows
   
2. Install the required dependencies

3. Run the script manually (optional):
   python get_forecast_flows.py <forecast_directory> <bias_correction> <forecast_directoryy>
   Set <bias_correction> to True to apply bias correction.

## Folder Structure

The project uses two primary folders for data storage and processing:

### `historical_data_download`

- **Purpose**: This folder stores the historical flow data, which is used for bias correction of the forecasted flow data.
- **Content**:
  - It includes data files that are downloaded once on 1st day of every month and then updated monthly as part of the bias correction process. These files are crucial for adjusting the forecasted data based on historical trends and patterns.
  - The data stored here is fetched from the DBHYDRO database, which contains historical records necessary for accurate bias correction.

### `forecasted_boundary_<month_year>`

- **Purpose**: This folder holds the forecasted flow data for the specified month and year, which is downloaded and processed daily using geoglow api.
- **Content**:
  - The folder is named based on the current month and year (e.g., `forecasted_boundary_08_2024` for August 2024) and contains files with forecasted flow data for each day.
  - If bias correction is enabled, the forecasted data in this folder will be adjusted using the historical data stored in the `historical_data_download` folder.
  - This folder is updated daily with new forecasted data, ensuring that the latest information is always available for analysis and decision-making.
