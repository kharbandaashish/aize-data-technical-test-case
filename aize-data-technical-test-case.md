# Problem Statement


## Geohashing

Your task is to transform the set of longitude, latitude coordinates provided in the `coordinates.csv.gz` file
into corresponding [GeoHash](https://en.wikipedia.org/wiki/Geohash) codes.

For each pair of coordinates, only the shortest geohash prefix that uniquely identifies this point must be stored.
For instance, this 3-point example will store these unique prefixes:

|latitude        | longitude       | geohash      | unique_prefix |
|----------------|-----------------|--------------|---------------|
|41.388828145321 | 2.1689976634898 | sp3e3qe7mkcb | sp3e3         |
|41.390743       | 2.138067        | sp3e2wuys9dr | sp3e2wuy      |
|41.390853       | 2.138177        | sp3e2wuzpnhr | sp3e2wuz      |


## Implementation

The solution should be coded in `Python` and you can use any open-source libraries.
For simplicity, it should work with any file respecting the same schema as the one provided and no database would be required to run the code.


## Architecture Design

How would you host and deploy a service for that solution? You can target any cloud provider service.

Explain the design and any choices/assumptions you've made.


## Evolution proposal

You are later asked to add a "[change data capture](https://en.wikipedia.org/wiki/Change_data_capture)" service in addition to the geohashing one.

Explain how you would design such a service, as you would in a technical proposal meeting with your coworkers.


## We value in the solution

- good software design
- use of data structures
- compliance with Python standards and modern usages
- instructions/documentation
- drawings/illustrations