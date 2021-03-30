# MariDataHarvest

MariDataHarvest is a tool for scrapping and harvesting Automatic Identification System (AIS) data provided by [marinecadastre](https://marinecadastre.gov/AIS/)
then appending it with the weather and environment conditions provided by [CMEMS](https://nrt.cmems-du.eu) and [RDA](rda.ucar.edu) at each geographical and UTC timestamp point.
In the following is a description of the datasets used:
[<img alt="Datasets Description" align="middle" src="./img/datasets_description.PNG"/>](https://docs.google.com/spreadsheets/d/1GxcBtnaAa2GQDwZibYFbWPXGi7BPpPdYLZwyetpsJOQ/edit?usp=sharing)

This tool is developed with in the [MariData](https://www.maridata.org) project.


## Required modules/libraries

For a detailed list, see section License below.

Install via pip:

```sh
pip install -r requirements.txt
```


## Usage

The script requires accounts for the following webservices:

- [CMEMS](https://resources.marine.copernicus.eu/?option=com_csw&task=results?option=com_csw&view=account)
- [RDA](https://rda.ucar.edu/index.html?hash=data_user&action=register)

The credentials of these services MUST be entered into a file called `.env.secret` as outlined here:

   ```sh
   UN_CMEMS=
   PW_CMEMS=
   UN_RDA=
   PW_RDA=
   ```

Start harvesting with the following command:
```sh
python main.py --year=2019 --minutes=30 --dir=C:\..
```

- `year`: the year to download AIS-data.

- `minutes`: is the subsampling interval in minutes.

- `dir`: the absolute path where to keep data. If empty, the directory is same as the project directory.

- `step` (optional): starts the script at a specific step:

  1. Download,
  2. Subsample,
  3. Appending weather data.

  If `step` equals `0` (default value), the script runs all steps starting from step 1.


### Docker

You can use the Dockerfile to build an docker image and run the script in its own isolated enviroment. It is recommend to provide a volume to persist the data between each run. You can specify the arguments `year`, `minutes`, and `dir` as environment variables when creating/starting the container as outlined in the following. The labels used are following the [Image And Container Label Specification](https://wiki.52north.org/Documentation/ImageAndContainerLabelSpecification) of 52°North.

1. Build:

   ```sh
   docker build \
      --build-arg GIT_COMMIT=$(git rev-parse -q --verify HEAD) \
      --build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") \
      -t 52north/mari-data_harvester:1.0.0 .
   ```

   Ensure, that the version of the image tag is matching the version in the Dockerfile, here: `1.0.0`.

1. Create named volume:

   ```sh
   docker volume create \
      --label org.52north.contact=dev-opser+maridata_harvester@example.org \
      --label org.52north.context="MariData Project: Data Harvesting Script" \
      --label org.52north.end-of-life="2021-12-31T23:59:59Z" \
      maridata-harvester_data
   ```


1. Run:

   ```sh
   docker run \
      --label org.52north.contact=dev-opser+maridata_harvester@example.org \
      --label org.52north.context="MariData Project: Data Harvesting Script" \
      --label org.52north.end-of-life="2021-12-31T23:59:59Z" \
      --volume maridata-harvester_data:/maridata/data \
      --volume $(pwd)/.env.secret:/maridata/.env.secret:ro \
      --name=mari-data_harvester \
      --env-file docker.env \
      --detach \
      52north/mari-data_harvester:1.0.0 \
      && docker logs --follow mari-data_harvester
   ```

   with `docker.env` containing the following information:

   ```sh
   YEAR=2019
   MINUTES=30
   DATA_DIR=/data
   STEP=0
   ```


## Deployment

Use the following command to send the code to any server for building the image and run it:

```sh
rsync --recursive --verbose --times --rsh ssh \
   --exclude='AIS-DATA' --exclude='*.tmp' \
   --exclude='*.swp' --exclude='.vscode' \
   --exclude='__pycache__' --delete . \
   mari-data-harvester.example.org:/home/user/maridata-harvester
```

## Contact
- [Zaabalawi, Sufian ](https://github.com/SufianZa)
- [Jürrens, Eike Hinderk](https://github.com/EHJ-52n)



## License

- [ ] Add License Header to source files

| Name            | Version   | License                                             |
|-----------------|-----------|-----------------------------------------------------|
| Fiona           | 1.8.18    | BSD License                                         |
| PyYAML          | 5.4.1     | MIT License                                         |
| Shapely         | 1.7.1     | BSD License                                         |
| attrs           | 20.3.0    | MIT License                                         |
| beautifulsoup4  | 4.9.3     | MIT License                                         |
| bs4             | 0.0.1     | MIT License                                         |
| certifi         | 2020.12.5 | Mozilla Public License 2.0 (MPL 2.0)                |
| cftime          | 1.4.1     | MIT License                                         |
| chardet         | 4.0.0     | GNU Library or Lesser General Public License (LGPL) |
| click           | 7.1.2     | BSD License                                         |
| click-plugins   | 1.1.1     | BSD License                                         |
| cligj           | 0.7.1     | BSD                                                 |
| geopandas       | 0.9.0     | BSD                                                 |
| idna            | 2.10      | BSD License                                         |
| motuclient      | 1.8.8     | GNU Lesser General Public License v3 (LGPLv3)       |
| munch           | 2.5.0     | MIT License                                         |
| netCDF4         | 1.5.6     | MIT License                                         |
| numpy           | 1.20.1    | BSD License                                         |
| pandas          | 1.2.3     | BSD                                                 |
| protobuf        | 3.15.6    | 3-Clause BSD License                                |
| pyproj          | 3.0.1     | MIT License                                         |
| python-dateutil | 2.8.1     | BSD License, Apache Software License                |
| python-dotenv   | 0.15.0    | BSD License                                         |
| pytz            | 2021.1    | MIT License                                         |
| requests        | 2.25.1    | Apache Software License                             |
| scipy           | 1.6.2     | BSD License                                         |
| siphon          | 0.9       | BSD License                                         |
| six             | 1.15.0    | MIT License                                         |
| soupsieve       | 2.2.1     | MIT License                                         |
| urllib3         | 1.26.4    | MIT License                                         |
| wget            | 3.2       | Public Domain                                       |
| xarray          | 0.15.1    | Apache Software License                             |

Generate this list via the following command:

```sh
docker run --rm --interactive --tty 52north/mari-data_harvester:1.0.0 /bin/bash \
   -c "pip install --no-warn-script-location --no-cache-dir pip-licenses > /dev/null && .local/bin/pip-licenses -f markdown"
```



## Funding

| Project/Logo | Description |
| :-------------: | :------------- |
| [<img alt="MariData" align="middle" width="267" height="50" src="./img/maridata_logo.png"/>](https://www.maridata.org/) | MariGeoRoute is funded by the German Federal Ministry of Economic Affairs and Energy (BMWi)[<img alt="BMWi" align="middle" width="144" height="72" src="./img/bmwi_logo_en.png" style="float:right"/>](https://www.bmvi.de/) |
