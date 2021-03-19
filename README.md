# MariDataHarvest

<<<<<<< HEAD
MariDataHarvest is a tool for scrapping and harvesting Automatic Identification System (AIS) data provided by [marinecadastre](https://marinecadastre.gov/AIS/) 
then appending it into the weather and environment conditions provided by [CMEMS](https://nrt.cmems-du.eu) and [RDA](rda.ucar.edu) at each geographical and temporal point.

## Required modules/libraries

- [ ] List required python libraries, maybe add `pip install lib1 lib2 lib3`
...🖊

=======
🚧🏗🚧👷🚧👷🚧🏗🚧👷🚧🏗🚧

- [ ] Add short oneliner about the script and what it is doing

Add description...🖊
...developed with in the [MariData](https://www.maridata.org) project.


## Required modules/libraries

- bs4
- beautifulsoup4
- motuclient
- pandas
- requests
- siphon
- wget
- xarray

Install via pip:

```sh
pip install -r requirements.txt
```
>>>>>>> 4d92710daf1b9dbdad326d6ba763f7e7c0344d9b

## Usage

Example:
```sh
python main.py --year=2019 --minutes=30 --work_dir=/path/..
```
both arguments are required:

>year: the year to download AIS-data.

>minutes: is the subsampling interval in minutes.

<<<<<<< HEAD
>work_dir: the absolute path where to keep data.

=======
>>>>>>> 4d92710daf1b9dbdad326d6ba763f7e7c0344d9b

### Docker

You can use the Dockerfile to build an docker image and run the script in its own isolated container. It is recommend to provide a volume to persist the data between each run. You can specify the arguments `year` and `minutes` as environment variables when creating/starting the container:

<<<<<<< HEAD
```sh
docker run --env YEAR=2019 --env MINUTES=30 --rm --volume $(pwd)/AIS-data:/data --name=mari-data_harvester 52north/mari-data_harvester:1.0.0
```
=======
1. Build:

   ```sh
   docker build --build-arg GIT_COMMIT=$(git rev-parse -q --verify HEAD) --build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") -t 52north/mari-data_harvester:1.0.0 .
   ```

   Ensure, that the version of the image tag is matching the version in the Dockerfile, here: `1.0.0`.

1. Run:

   ```sh
   docker run --env YEAR=2019 --env MINUTES=30 --rm --volume $(pwd)/AIS-data:/data --name=mari-data_harvester 52north/mari-data_harvester:1.0.0
   ```
>>>>>>> 4d92710daf1b9dbdad326d6ba763f7e7c0344d9b


## Contact

<<<<<<< HEAD
- [ ] name persons to contact if any questions regarding the code occur (best: link to github user account)
...🖊
=======
- [Sufian Zaabalawi](https://github.com/SufianZa)
- [Jürrens, Eike Hinderk](https://github.com/EHJ-52n)
>>>>>>> 4d92710daf1b9dbdad326d6ba763f7e7c0344d9b


## License

- [ ] Add License Header to source files
- [ ] Add license file and information to repository
<<<<<<< HEAD
# gpl apache 
...🖊

=======

...🖊


## Funding

| Project/Logo | Description |
| :-------------: | :------------- |
| [<img alt="MariData" align="middle" width="267" height="50" src="./img/maridata_logo.png"/>](https://www.maridata.rg/) | MariGeoRoute is funded by the German Federal Ministry of Economic Affairs and Energy (BMWi)[<img alt="BMWi" align="middle" width="144" height="72" src="./img/bmwi_logo_en.png"/>](https://www.bmvi.de/) |

🚧🏗🚧👷🚧👷🚧🏗🚧👷🚧🏗🚧
>>>>>>> 4d92710daf1b9dbdad326d6ba763f7e7c0344d9b
