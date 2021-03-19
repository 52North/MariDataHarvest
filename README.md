# MariDataHarvest

MariDataHarvest is a tool for scrapping and harvesting Automatic Identification System (AIS) data provided by [marinecadastre](https://marinecadastre.gov/AIS/) 
then appending it into the weather and environment conditions provided by [CMEMS](https://nrt.cmems-du.eu) and [RDA](rda.ucar.edu) at each geographical and temporal point.

## Required modules/libraries

- [ ] List required python libraries, maybe add `pip install lib1 lib2 lib3`
...🖊


## Usage

Example:
```sh
python main.py --year=2019 --minutes=30 --work_dir=/path/..
```
both arguments are required:

>year: the year to download AIS-data.

>minutes: is the subsampling interval in minutes.

>work_dir: the absolute path where to keep data.


### Docker

You can use the Dockerfile to build an docker image and run the script in its own isolated container. It is recommend to provide a volume to persist the data between each run. You can specify the arguments `year` and `minutes` as environment variables when creating/starting the container:

```sh
docker run --env YEAR=2019 --env MINUTES=30 --rm --volume $(pwd)/AIS-data:/data --name=mari-data_harvester 52north/mari-data_harvester:1.0.0
```


## Contact

- [ ] name persons to contact if any questions regarding the code occur (best: link to github user account)
...🖊


## License

- [ ] Add License Header to source files
- [ ] Add license file and information to repository
# gpl apache 
...🖊

