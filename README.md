# MariDataHarvest

🚧🏗🚧👷🚧👷🚧🏗🚧👷🚧🏗🚧

MariDataHarvest is a tool for scrapping and harvesting Automatic Identification System (AIS) data provided by [marinecadastre](https://marinecadastre.gov/AIS/) 
then appending it with the weather and environment conditions provided by [CMEMS](https://nrt.cmems-du.eu) and [RDA](rda.ucar.edu) at each geographical and UTC timestamp point.

This tool is developed with in the [MariData](https://www.maridata.org) project.


## Required modules/libraries

- beautifulsoup4
- bs4
- motuclient
- netCDF4
- pandas
- pyyaml
- requests
- scipy
- siphon
- wget
- xarray

Install via pip:

```sh
pip install -r requirements.txt
```

## Usage

Example:
```sh
python main.py --year=2019 --minutes=30 --dir=C:\.. --step=0
```
both arguments `minutes` and `year` are required:

>year: the year to download AIS-data.

>minutes: is the subsampling interval in minutes.

>dir: the absolute path where to keep data. If empty, the directory is same as the project directory. 

>step: starts the script at a specific step (1. Download, 2. Subsample, 3. Appending weather data). If equals 0, the script runs all steps starting from step 1. 



### Docker
- [ ] add --dir to docker arguments

You can use the Dockerfile to build an docker image and run the script in its own isolated container. It is recommend to provide a volume to persist the data between each run. You can specify the arguments `year` and `minutes` as environment variables when creating/starting the container:

1. Build:

   ```sh
   docker build --build-arg GIT_COMMIT=$(git rev-parse -q --verify HEAD) --build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") -t 52north/mari-data_harvester:1.0.0 .
   ```

   Ensure, that the version of the image tag is matching the version in the Dockerfile, here: `1.0.0`.

1. Run:

   ```sh
   docker run --env YEAR=2019 --env MINUTES=30 --rm --volume $(pwd)/AIS-data:/data --name=mari-data_harvester 52north/mari-data_harvester:1.0.0
   ```

## Contact
- [Zaabalawi, Sufian ](https://github.com/SufianZa)
- [Jürrens, Eike Hinderk](https://github.com/EHJ-52n)



## License

- [ ] Add License Header to source files

| Name            | Version   | License                                             |
|-----------------|-----------|-----------------------------------------------------|
| beautifulsoup4  | 4.9.3     | MIT License                                         |
| bs4             | 0.0.1     | MIT License                                         |
| certifi         | 2020.12.5 | Mozilla Public License 2.0 (MPL 2.0)                |
| cftime          | 1.4.1     | MIT License                                         |
| chardet         | 4.0.0     | GNU Library or Lesser General Public License (LGPL) |
| idna            | 2.10      | BSD License                                         |
| motuclient      | 1.8.8     | GNU Lesser General Public License v3 (LGPLv3)       |
| netCDF4         | 1.5.6     | MIT License                                         |
| numpy           | 1.20.1    | BSD License                                         |
| pandas          | 1.2.3     | BSD                                                 |
| protobuf        | 3.15.6    | 3-Clause BSD License                                |
| scipy           |  1.6.1    | BSD License                                         |
| python-dateutil | 2.8.1     | BSD License, Apache Software License                |
| pytz            | 2021.1    | MIT License                                         |
| requests        | 2.25.1    | Apache Software License                             |
| siphon          | 0.9       | BSD License                                         |
| six             | 1.15.0    | MIT License                                         |
| soupsieve       | 2.2.1     | MIT License                                         |
| urllib3         | 1.26.4    | MIT License                                         |
| wget            | 3.2       | Public Domain                                       |
| xarray          | 0.15.1    | Apache Software License                             |



## Funding

| Project/Logo | Description |
| :-------------: | :------------- |
| [<img alt="MariData" align="middle" width="267" height="50" src="./img/maridata_logo.png"/>](https://www.maridata.org/) | MariGeoRoute is funded by the German Federal Ministry of Economic Affairs and Energy (BMWi)[<img alt="BMWi" align="middle" width="144" height="72" src="./img/bmwi_logo_en.png"/>](https://www.bmvi.de/) |

🚧🏗🚧👷🚧👷🚧🏗🚧👷🚧🏗🚧
