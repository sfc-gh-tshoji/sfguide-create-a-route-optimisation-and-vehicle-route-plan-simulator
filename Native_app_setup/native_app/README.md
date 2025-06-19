# OPENROUTESERVICE SNOWFLAKE NATIVE APP
For more background, please refer to the Medium article.

## Prerequisites
- Snowflake Account: Please note that trial accounts are not supported.
- Docker Desktop: For installation instructions, see https://docs.docker.com/get-docker/. 

## Deployment steps

1. **Environment Setup:** In Snowsight execute `provider_setup/env_setup.sql` to create database, image repository and other Snowflake objects.
2. **Snowflake CLI Connection:** Create a Snowflake CLI connection following the [tutorial](https://docs.snowflake.com/en/developer-guide/native-apps/tutorials/getting-started-tutorial#create-a-snowflake-cli-connection-for-the-tutorial). 
3. **Upload Map and Configuration File:** Upload the map file (`SanFrancisco.osm.pbf` or your custom `.osm.pbf` map) and `ors-config.yaml` file to the internal stage `ors_spcs_stage` created in Step 1. Refresh stage metadata after uploading the map via UI or via code `ALTER STAGE REFRESH CORE.ORS_SPCS_STAGE REFRESH;`
* You can use the example files: `SanFrancisco.osm.pbf` and `ors-config.yaml` located in `provider_setup/staged_files`. The San Francisco map uploaded to the repository originates from [geofabrik](download.geofabrik.de). This website provides a wide selection of maps.
* If using a custom map, ensure the `source_file` field within `ors-config.yml` is updated to reflect the new map filename. 
* **Map Upload Methods**:
  * For maps **below 250MB**: upload using the [web interface](https://docs.snowflake.com/en/user-guide/data-load-web-ui)
  * For maps **below 5GB**: use [snow stage copy](https://docs.snowflake.com/en/developer-guide/snowflake-cli/command-reference/stage-commands/copy) or [PUT](https://docs.snowflake.com/en/sql-reference/sql/put) command.
  * For maps **above 5GB**: load them into cloud storage bucket (e.g.,S3), create an external stage, and then copy them into the internal stage using [copy files](https://docs.snowflake.com/en/sql-reference/sql/copy-files) command.

4. **Image Loading:** From your project's working directory, execute the terminal script `provider_setup/spcs_setup.sh`. Update `<CONNECTION_NAME>` with the name of the connection you created in Step 2. This script will load the necessary images into the image repository.
5. **Application Installation:** From your project's working directory, execute snowflake CLI command: `snow app run -c <CONNECTION_NAME>`. Afterwards, in Snowsight, in the navigation bar on the left, click **Data Products >> Apps**. Select the application, grant it the required privileges via the UI and activate it via button in upper right corner. Launching it for the first time might take a minute or two.
6. **API Testing Examples:**
After launching the application you will see a simple streamlit app containing examples how to test the APIs.

## Tips for configuration, optimisation and troubleshooting

Whenever there is an issue with the application, checking the [container logs](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#label-snowpark-containers-working-with-services-local-logs) is recommended as the first step.

1. **Autosuspend:** The service function is configured to auto-suspend after 4 hours to prevent cost overruns. Since auto-resume is not supported for service-to-service communication yet (on the roadmap), **manual resumption is necessary after suspension**. If you have only routing-gateway container running, you will get an error if the other services are suspended. 
```
ALTER SERVICE CORE.ORS_SERVICE RESUME; 
ALTER SERVICE CORE.ROUTING_GATEWAY_SERVICE RESUME; 
ALTER SERVICE CORE.VROOM_SERVICE RESUME;
ALTER SERVICE CORE.DOWNLOADER RESUME;
```

2. **Graphs and elevation creation for different driving profiles:** After the openrouteservice starts, it will create graphs and elevation cache based on the map file and `ors-config.yaml` files loaded to the internal stage `ORS_SPCS_STAGE`

If you use the default settings from the example provided, you should see three folders in the graps stage (`ors_graphs_spcs_stage`):
* driving-car
* cycling-road
* driving-hgv. 

To see the latest files, please remember to refresh the directory table for the stage. For larger maps, the graph creation process might fail due to Java memory allocation limits. This failure will be visible in the container logs. You can increase this limit by adjusting the field: `XMX` within the openrouteservice service definition available under `services/openrouteservice/openrouteservice.yaml`. The blueprint for openrouteservice service definition is stored [here](https://github.com/GIScience/openrouteservice/blob/main/docker-compose.yml). Additionally, you might need to increase the compute pool size for big maps.

3. **Snowpark Container Services Scaling:** Both [compute pools](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-compute-pool#autoscaling-of-compute-pool-nodes) and [services](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-services#scaling-services) can be scaled.

4. **Configuration of `ors-config.yaml` file:** The blueprint for this config file is available [here](https://github.com/GIScience/openrouteservice/blob/main/ors-config.yml).

Openrouteservice offers different profiles that can be used to find routes. Based on settings from `ors-config.yaml` three profiles from many available were enabled by default for the presented solution: car (driving-car), cycling (cycling-road) and heavy good vehicles (driving-hgv). **Please note that the source_file must point to the right map**

```yaml
ors:
 engine:
   profile_default:
     build: 
       source_file: /home/ors/files/SanFrancisco.osm.pbf
       instructions: false
   profiles:
     driving-car:
       enabled: true
     cycling-road:
       enabled: true
     driving-hgv:
       enabled: true
```

Additionally, `ors-config.yaml` contains multiple default settings that might limit the application capabilities, unless you explicitly overwrite them. During internal testing, we have observed an error in container logs when running an optimization API for a heavy workload:

`[Error] Unable to compute a distance/duration matrix: Search exceeds the limit of visited nodes.`

It was solved by changing the value of the parameter to `maximum_visited_nodes:: 100000` in the `ors-config.yaml`

