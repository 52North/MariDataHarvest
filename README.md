# MariDataHarvest

## TODOS - Repository-Reconstruction

- [ ] `requirements.txt`: one for all or all one
- [ ] `README.md`: one for all or all one
- [ ] Add Package Overview to root `README.md`
- [ ] Update dependency list generation step incl. one for all or all one
- [ ] Update `docker-compose.yml` after repo restructuring
- [ ] Rename repo to ???
- [ ] `Dockerfile` of harvester:
  - [ ] copy package and the dependency packages
  - [ ] adjust `CMD` to match new python package structure
- [ ] Add `Dockerfile` for EnvDataServer
- [ ] Add venv instructions (?)

<div style="float:right; border: 1px solid #cecece; padding: 5px;">
<!-- TOC -->

- [MariDataHarvest](#maridataharvest)
    - [TODOS - Repository-Reconstruction](#todos---repository-reconstruction)
    - [Requirements](#requirements)
    - [Usage](#usage)
    - [Docker](#docker)
        - [CODE-DE](#code-de)
    - [Deployment](#deployment)
        - [Web Access to the Data](#web-access-to-the-data)
    - [Contact](#contact)
    - [License](#license)
    - [Funding](#funding)

<!-- /TOC -->
</div>

MariDataHarvest is a tool for scrapping and harvesting Automatic Identification System (AIS) data provided by [marinecadastre](https://marinecadastre.gov/AIS/)
then appending it with the weather and environment conditions provided by [CMEMS](https://nrt.cmems-du.eu) and [RDA](https://rda.ucar.edu/index.html) at each geographical and UTC timestamp point.
In the following is a description of the datasets used:

* <details>
  <summary>Datasets Description</summary>

  [<img alt="Datasets Description" align="middle" src="./img/datasets_description.PNG"/>](https://docs.google.com/spreadsheets/d/1GxcBtnaAa2GQDwZibYFbWPXGi7BPpPdYLZwyetpsJOQ/edit?usp=sharing)

  </details>

* <details>
  <summary>Variables Description</summary>

  [<img alt="Variables Description" align="middle" src="./img/variables_description.PNG"/>](https://docs.google.com/spreadsheets/d/1GxcBtnaAa2GQDwZibYFbWPXGi7BPpPdYLZwyetpsJOQ/edit?usp=sharing)

  </details>

This tool is developed with in the [MariData](https://www.maridata.org) project.


## Requirements

MariDataHarvest requires __python 3__ and __pip__ to run. You can install all python requirements with the following command:

```sh
pip install -r requirements.txt
```

For a detailed list, see section License below.


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

- `year`: the year(s) to download AIS-data. Expected input a year 'YYYY' , a range of years 'YYYY-YYYY' or multiple years 'YYYY,YYYY,YYYY'.

- `minutes`: is the sub-sampling interval in minutes.

- `dir`: the absolute path where to keep data. If empty, the directory is same as the project directory.

- **optional** arguments:

  - `step`: starts the script at a specific step:

    1. Download,
    2. Subsample,
    3. Appending weather data.

    If `step` equals `0` (default value), the script runs all steps starting from step 1.

  - `clear`: clears files of `year` ONLY after step 2 is done.

  - `depth_first`: runs all steps for each file, which automatically deactivates `step` argument.

## Docker

You can use the [Dockerfile](./Dockerfile) to build a docker image and run the script in its own isolated environment. It is recommend to provide a volume to persist the data between each run. You can specify all arguments including the optional ones as environment variables when creating/starting the container as outlined in the following. The labels used are following the [Image And Container Label Specification](https://wiki.52north.org/Documentation/ImageAndContainerLabelSpecification) of 52??North.

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
      --label org.52north.contact=dev-opser+mari-data_harvester@example.org \
      --label org.52north.context="MariData Project: Data Harvesting Script" \
      --label org.52north.end-of-life="2021-12-31T23:59:59Z" \
      mari-data-harvester_data
   ```


1. Run:

   ```sh
   docker run \
      --label org.52north.contact=dev-opser+mari-data_harvester@example.org \
      --label org.52north.context="MariData Project: Data Harvesting Script" \
      --label org.52north.end-of-life="2021-12-31T23:59:59Z" \
      --volume mari-data-harvester_data:/mari-data/data \
      --volume $(pwd)/.env.secret:/mari-data/.env.secret:ro \
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
   DATA_DIR=/mari-data/data
   STEP=0
   DEPTH_FIRST=--depth-first
   CLEAR=--clear
   ```


### CODE-DE

You can use "local" CMEMS data when running on a [CODE-DE VM](https://code-de.org/en/portfolio/33?q=infrastructure). After requesting access to the data via the [support form](https://code-de.org/en/helpdesk), you can follow the instructions to [access the CREODIAS repository](https://code-de.org/en/help/topic/manual/X4bBsxEAADWjnZas). As a result, the CMEMS data will be available on your VM under `/eodata/CMEMS`.

This directory needs to be mounted into the container, hence the following volume specification must be added to the `docker run` call:

```sh
--volume /eodata/CMEMS:/eodata/CMEMS:ro
```


## Deployment

Use the following command to send the code to any server for building the image (or clone this repository using `git clone...`) and run it:

```sh
rsync --recursive --verbose --times --rsh ssh \
   --exclude='AIS-DATA' --exclude='*.tmp' \
   --exclude='*.swp' --exclude='.vscode' \
   --exclude='__pycache__' --delete . \
   mari-data-harvester.example.org:/home/user/mari-data-harvester
```

### Web Access to the Data

We are using an nginx container to provide web access to the generated data. It requires an external service to maintain the ssl certificates. The according data volume is mounted read-only and externally provided, hence docker-compose does not create it with a project prefix.

Just execute the following command in root folder of the repository to start the service:

```sh
docker-compose up -d --build && docker-compose logs --follow
```

The data is available directly at the server root via https. All requests to http are redirected to https by default.

## Contact
- [Zaabalawi, Sufian ](https://github.com/SufianZa)
- [J??rrens, Eike Hinderk](https://github.com/EHJ-52n)


## License

- [ ] Add License Header to source files

| Name            | Version   | License                                             |
|-----------------|-----------|-----------------------------------------------------|
| Fiona           | 1.8.19    | BSD License                                         |
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
| cloudpickle     | 1.6.0     | BSD License                                         |
| dask            | 2021.4.0  | BSD License                                         |
| fsspec          | 0.9.0     | BSD License                                         |
| geopandas       | 0.9.0     | BSD                                                 |
| idna            | 2.10      | BSD License                                         |
| locket          | 0.2.1     | BSD License                                         |
| motuclient      | 1.8.8     | GNU Lesser General Public License v3 (LGPLv3)       |
| munch           | 2.5.0     | MIT License                                         |
| netCDF4         | 1.5.6     | MIT License                                         |
| numpy           | 1.20.1    | BSD License                                         |
| pandas          | 1.2.4     | BSD                                                 |
| partd           | 1.2.0     | BSD                                                 |
| protobuf        | 3.16.0rc1 | 3-Clause BSD License                                |
| pyproj          | 3.0.1     | MIT License                                         |
| python-dateutil | 2.8.1     | BSD License, Apache Software License                |
| python-dotenv   | 0.17.0    | BSD License                                         |
| pytz            | 2021.1    | MIT License                                         |
| requests        | 2.25.1    | Apache Software License                             |
| scipy           | 1.6.2     | BSD License                                         |
| siphon          | 0.9       | BSD License                                         |
| six             | 1.15.0    | MIT License                                         |
| soupsieve       | 2.2.1     | MIT License                                         |
| toolz           | 0.11.1    | BSD License                                         |
| urllib3         | 1.26.4    | MIT License                                         |
| xarray          | 0.17.0    | Apache Software License                             |

<details>
<summary>generate license list</summary>

```sh
docker run --rm --interactive --tty 52north/mari-data_harvester:1.0.0 /bin/bash \
   -c "pip install --no-warn-script-location --no-cache-dir pip-licenses > /dev/null && .local/bin/pip-licenses -f markdown"
```

</details>


## Funding

| Project/Logo | Description |
| :-------------: | :------------- |
| [<img alt="MariData" align="middle" width="267" height="50" src="https://52north.org/delivery/MariData/img/maridata_logo.png"/>](https://www.maridata.org/) | MariGeoRoute is funded by the German Federal Ministry of Economic Affairs and Energy (BMWi)[<img alt="BMWi" align="middle" width="144" height="72" src="https://52north.org/delivery/MariData/img/bmwi_logo_en.png" style="float:right"/>](https://www.bmvi.de/) |
