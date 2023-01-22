# Collectors

This repo contains all the scripts that are run to scrape real estate websites. Each conuntry has a dedicated folder, where it is possible to find a city.csv file with the list of cities to probe. Each city is provided with the coordinates of a bounding box wrapping the main area. Other cities can be added just extending the csv file with additional rows. The results are stored in csv files which will be both saved locally and pushed in a shared Google Drive.

## How to run the code

To run the scripts must activate the environment `findap` running the command `conda activate findap`. If the environment is not created, please ensure to create it running the command `conda env create -f environment.yml`.
